import time
from typing import List

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns

# import seaborn as sns
from loguru import logger

from qsimpy.core import Model, Sink
from qsimpy.gym import GymSink, MultihopGymSource
from qsimpy.random import Gamma
from qsimpy.simplemultihop import SimpleMultiHop

# Create the QSimPy environment
# a class for keeping all of the entities and accessing their attributes
model = Model(name="test multihop gym")

N_HOPS = 4
# create the gym source
source = MultihopGymSource(
    name="start-node",
    n_hops=N_HOPS,
    main_task_type="main",
    main_task_num=[1, 0, 0, 0],
    traffic_task_type="traffic",
    traffic_task_num=[5, 5, 5, 5],
    traffic_task_ldp=[1.0, 1.0, 1.0, 1.0],
)
model.add_entity(source)

services: List[Gamma] = []
# service process a Gamma distribution
avg_service_rate = 0.10
for hop in range(N_HOPS):
    # Queue and Server
    # service process a HeavyTailGamma
    service = Gamma(
        shape=5.00,
        scale=0.5,
        seed=120034 + hop * 23400,
        dtype="float64",
    )
    services.append(service)

queue = SimpleMultiHop(
    name="queue",
    n_hops=N_HOPS,
    service_rp=services,
)
model.add_entity(queue)

# create the sinks
sink = GymSink(
    name="gym-sink",
    batch_size=10000,
)
# define postprocess function: the name must be 'user_fn'


def user_fn(df):
    # df is pandas dataframe in batch_size
    df["end2end_delay"] = df["end_time"] - df["start_time"]

    # process service delay, queue delay
    for h in range(N_HOPS):
        df[f"service_delay_{h}"] = df[f"end_time_{h}"] - df[f"service_time_{h}"]
        df[f"queue_delay_{h}"] = df[f"service_time_{h}"] - df[f"queue_time_{h}"]

    # process time-in-service
    # h is hop num
    for h in range(N_HOPS):
        # reduce queue_length by 1
        df[f"queue_length_h{h}"] = df[f"queue_length_h{h}"] - 1
        # process longer_delay_prob here for benchmark purposes
        df[f"longer_delay_prob_h{h}"] = source.traffic_task_ldp[h]
        del df[f"last_service_time_h{h}"], df[f"queue_is_busy_h{h}"]

    # delete remaining items
    for h in range(N_HOPS):
        del df[f"end_time_{h}"], df[f"service_time_{h}"], df[f"queue_time_{h}"]

    return df


sink._post_process_fn = user_fn
model.add_entity(sink)

drop_sink = Sink(
    name="drop-sink",
)
model.add_entity(drop_sink)

# Wire start-node, queue, end-node, and sink together
source.out = queue.name
queue.out = sink.name
queue.drop = drop_sink.name
sink.out = source.name


# Setup task records
# timestamps
timestamps = {
    source.name: {
        "task_generation": "start_time",
    },
    sink.name: {
        "task_reception": "end_time",
    },
    queue.name: {},
}
for hop in range(N_HOPS):
    timestamps[queue.name].update(
        {
            f"task_reception_h{hop}": f"queue_time_{hop}",
            f"service_start_h{hop}": f"service_time_{hop}",
            f"service_end_h{hop}": f"end_time_{hop}",
        }
    )
# attributes
attributes = {
    source.name: {
        "task_generation": {queue.name: {}},
    },
    queue.name: {f"service_end_h{hop}": {queue.name: {}} for hop in range(N_HOPS - 1)},
}
for hop in range(N_HOPS):
    attributes[source.name]["task_generation"][queue.name].update(
        {
            f"queue_length_h{hop}": f"queue_length_h{hop}",
            f"last_service_time_h{hop}": f"last_service_time_h{hop}",
            f"is_busy_h{hop}": f"queue_is_busy_h{hop}",
        }
    )

# set attributes and timestamps
model.set_task_records(
    {
        "timestamps": timestamps,
        "attributes": attributes,
    }
)

# prepare for run
model.prepare_for_run(debug=False)


# report timesteps and until proc
def until_proc(env, samples, step, report_samples_perc):
    old_perc = 0
    while True:
        yield env.timeout(step)
        progress_in_perc = int(
            float(sink.attributes["tasks_received"]) / float(samples) * 100
        )
        if (progress_in_perc % report_samples_perc == 0) and (
            old_perc != progress_in_perc
        ):
            logger.info(" Simulation progress" + f" {progress_in_perc}% done")
            old_perc = progress_in_perc
        if sink.attributes["tasks_received"] >= samples:
            break
    return True


# Run!
logger.info("Run started")
start = time.time()
model.env.run(
    until=model.env.process(
        until_proc(
            model.env,
            samples=1000,
            step=10,
            report_samples_perc=1,
        )
    )
)
end = time.time()
print("Run finished in {0} seconds".format(end - start))

print("Source generated {0} tasks".format(source.get_attribute("main_tasks_generated")))
print(
    "Queue completed {0}, dropped {1}".format(
        queue.get_attribute(f"tasks_completed_h{N_HOPS-1}"),
        queue.get_attribute(f"tasks_dropped_h{N_HOPS-1}"),
    )
)
print(" Sink received {0} tasks".format(sink.get_attribute("tasks_received")))

start = time.time()

# Process the collected data
df = sink.received_tasks
print(df)

df.write_parquet(
    file="records.parquet",
    compression="snappy",
)
df.write_csv(
    "records.csv",
    sep=",",
)
df_dropped = df.filter(pl.col("end_time") == -1)
df_finished = df.filter(pl.col("end_time") >= 0)
# df = df_finished

res = []
pd_df = df.to_pandas()
sns.set_style("darkgrid")

sns.displot(df["end2end_delay"], kde=True)
plt.savefig("end2end_total.png")

for hop in range(N_HOPS):
    sns.displot(df[f"service_delay_{hop}"], kde=True)
    plt.savefig(f"service_{hop}.png")

    sns.displot(df[f"queue_delay_{hop}"], kde=True)
    plt.savefig(f"queue_delay_{hop}.png")
