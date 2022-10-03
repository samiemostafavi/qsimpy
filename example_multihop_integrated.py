import os
import time
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns

# import seaborn as sns
from loguru import logger

from qsimpy.core import Model, TimedSource
from qsimpy.polar import PolarSink
from qsimpy.random import Deterministic, Gamma
from qsimpy.simplemultihop import SimpleMultiHop

# Create the QSimPy environment
# a class for keeping all of the entities and accessing their attributes
model = Model(name="test multihop aqm")

# Create a source
# arrival process deterministic
arrival = Deterministic(
    rate=0.098,
    seed=100234,
    dtype="float64",
)
source = TimedSource(
    name="start-node",
    arrival_rp=arrival,
    task_type="0",
    delay_bound=202.55575775238685,
)
model.add_entity(source)

N_HOPS = 4
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

# Sink: to capture both finished tasks and dropped tasks (PolarSink to be faster)
sink = PolarSink(
    name="sink",
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
    # p is predictor num, h is hop num
    for p in range(N_HOPS):
        for j in range(N_HOPS - p):
            h = j + p

            # process time in service
            df[f"time_in_service_p{p}_h{h}"] = df.apply(
                lambda row: (
                    row[f"queue_time_{p}"] - row[f"last_service_time_p{p}_h{h}"]
                )
                if row[f"queue_is_busy_p{p}_h{h}"]
                else None,
                axis=1,
            ).astype("float64")

            # process longer_delay_prob here for benchmark purposes
            service = Gamma(**services[h])
            df[f"longer_delay_prob_p{p}_h{h}"] = np.float64(1.00) - service.cdf(
                y=df[f"time_in_service_p{p}_h{h}"].to_numpy(),
            )
            df[f"longer_delay_prob_p{p}_h{h}"] = df[
                f"longer_delay_prob_p{p}_h{h}"
            ].fillna(np.float64(0.00))
            del df[f"last_service_time_p{p}_h{h}"], df[f"queue_is_busy_p{p}_h{h}"]

    # delete remaining items
    for h in range(N_HOPS):
        del df[f"end_time_{h}"], df[f"service_time_{h}"], df[f"queue_time_{h}"]

    return df


sink._post_process_fn = user_fn
model.add_entity(sink)

# Wire start-node, queue, end-node, and sink together
source.out = queue.name
queue.out = sink.name
queue.drop = sink.name

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
            f"queue_length_h{hop}": f"queue_length_p0_h{hop}",
            f"last_service_time_h{hop}": f"last_service_time_p0_h{hop}",
            f"is_busy_h{hop}": f"queue_is_busy_p0_h{hop}",
        }
    )
for p in range(1, N_HOPS):
    for hop in range(p, N_HOPS):
        attributes[queue.name][f"service_end_h{p-1}"][queue.name].update(
            {
                f"queue_length_h{hop}": f"queue_length_p{p}_h{hop}",
                f"last_service_time_h{hop}": f"last_service_time_p{p}_h{hop}",
                f"is_busy_h{hop}": f"queue_is_busy_p{p}_h{hop}",
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

# run configuration
until = 1000000  # 100000
report_state_frac = 0.01  # every 1% report


# report timesteps
def report_state(time_step):
    yield model.env.timeout(time_step)
    logger.info(f"Simulation progress {100.0*float(model.env.now)/float(until)}% done")


for step in np.arange(0, until, float(until) * float(report_state_frac), dtype=int):
    model.env.process(report_state(step))

project_path = "projects/newdelta_limited_test"
os.makedirs(project_path, exist_ok=True)

modeljson = model.json()
with open(
    project_path + "/model.json",
    "w",
    encoding="utf-8",
) as f:
    f.write(modeljson)

# Run!
start = time.time()
model.env.run(until=until)
end = time.time()
print("Run finished in {0} seconds".format(end - start))

print("Source generated {0} tasks".format(source.get_attribute("tasks_generated")))
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
    file=project_path + "/records.parquet",
    compression="snappy",
)

df_dropped = df.filter(pl.col("end_time") == -1)
df_finished = df.filter(pl.col("end_time") >= 0)
# df = df_finished

res = []
pd_df = df.to_pandas()
all_missed = len(
    pd_df[(pd_df["end2end_delay"] > source.delay_bound) | (pd_df["end_time"] == -1)]
)
print(f"{all_missed/len(pd_df)} fraction of tasks failed.")

sns.set_style("darkgrid")

sns.displot(df["end2end_delay"], kde=True)
plt.savefig("end2end_total.png")

for hop in range(N_HOPS):
    sns.displot(df[f"service_delay_{hop}"], kde=True)
    plt.savefig(f"service_{hop}.png")

    sns.displot(df[f"queue_delay_{hop}"], kde=True)
    plt.savefig(f"queue_delay_{hop}.png")
