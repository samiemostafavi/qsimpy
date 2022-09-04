import time

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from qsimpy.core import Model
from qsimpy.discrete import CapacityQueue, CapacitySource, Deterministic, Rayleigh
from qsimpy.polar import PolarSink

# Create the QSimPy environment
# a class for keeping all of the entities and accessing their attributes
model = Model(name="test Rayleigh model")

# arrival process uniform
arrival = Deterministic(
    seed=0,
    rate=25,
    dtype="float64",
)
# Create a source
source = CapacitySource(
    name="start-node",
    arrival_rp=arrival,
    task_type="0",
)
model.add_entity(source)

# service process is Rayleigh channel capacity
service = Rayleigh(
    seed=120034,
    snr=5,  # in db
    bandwidth=20e3,  # in hz
    time_slot_duration=1e-3,  # in seconds
    dtype="float64",
)
# a queue
queue = CapacityQueue(
    name="queue",
    service_rp=service,
    queue_limit=None,
)
model.add_entity(queue)

# a sink: to capture both finished tasks and dropped tasks
sink = PolarSink(
    name="sink",
    batch_size=10000,
)
model.add_entity(sink)

# add task records
model.set_task_records(
    {
        "timestamps": {
            source.name: {
                "task_generation": "start_time",
            },
            queue.name: {
                "task_reception": "queue_time",
                "service_time": "end_time",
            },
        },
        "attributes": {
            source.name: {
                "task_generation": {
                    queue.name: {
                        "queue_length": "queue_length",
                    },
                },
            },
        },
    }
)

# setup routings
source.out = queue.name
queue.out = sink.name
queue.drop = sink.name

model.prepare_for_run(debug=False)

# Run it
print("Run the model:")
start = time.time()
model.env.run(until=10000)
end = time.time()
print("Run finished in {0} seconds".format(end - start))

print("Source generated {0} tasks".format(source.get_attribute("tasks_generated")))
print(
    "Queue completed {0}, dropped {1}".format(
        queue.get_attribute("tasks_completed"),
        queue.get_attribute("tasks_dropped"),
    )
)
print("Sink received {0} tasks".format(sink.get_attribute("tasks_received")))

# Process the collected data
df = pd.DataFrame(sink.received_tasks)

df_dropped = df[df.end_time == -1]
df_finished = df[df.end_time >= 0]
df = df_finished

df["end2end_delay"] = df["end_time"] - df["start_time"]
df["queue_delay"] = df["end_time"] - df["queue_time"]

del df["end_time"]
del df["start_time"]
del df["queue_time"]


# plot end-to-end delay profile
sns.set_style("darkgrid")
sns.displot(df["end2end_delay"], kde=True)
plt.savefig("end2end.png")

sns.displot(df["queue_delay"], kde=True)
plt.savefig("queue_delay.png")

print(df["end2end_delay"].describe(percentiles=[0.9, 0.99, 0.999, 0.9999, 0.99999]))
print(df["queue_delay"].describe(percentiles=[0.9, 0.99, 0.999, 0.9999, 0.99999]))
