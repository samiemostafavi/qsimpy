import functools
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

import qsimpy

if __name__ == "__main__":

    # arrival = functools.partial(random.expovariate, 0.8)
    # arrival = functools.partial(random.uniform, 1.1, 1.1)
    # service = functools.partial(random.expovariate, 1)

    arrival_rate = 0.095
    rng_arrival = np.random.default_rng(100234)
    arrival = functools.partial(
        rng_arrival.uniform, 1.00 / arrival_rate, 1.00 / arrival_rate
    )

    # Gamma distribution
    avg_service_rate = 0.10
    rng_service = np.random.default_rng(120034)
    service = functools.partial(rng_service.gamma, 1.00 / avg_service_rate, 1)

    # Create the QSimPy environment
    # a class for keeping all of the entities and accessing their attributes
    env = qsimpy.Environment(name="0")

    # Create a source
    source = qsimpy.Source(
        name="start-node",
        env=env,
        arrival_dist=arrival,
        task_type="0",
    )

    # a queue
    queue = qsimpy.SimpleQueue(
        name="queue",
        env=env,
        service_dist=service,
        queue_limit=None,
    )

    # a sink: to capture both finished tasks and dropped tasks
    sink = qsimpy.Sink(
        name="sink",
        env=env,
        debug=False,
    )

    # Wire start-node, queue, end-node, and sink together
    source.out = queue
    queue.out = sink
    queue.drop = sink

    env.task_records = {
        "timestamps": {
            source.name: {
                "task_generation": "start_time",
            },
            queue.name: {
                "task_reception": "queue_time",
                "service_start": "service_time",
                "service_end": "end_time",
            },
        },
        "attributes": {
            source.name: {
                "task_generation": {
                    queue.name: {
                        "queue_length": "queue_length",
                        "last_service_duration": "last_service_duration",
                        "last_service_time": "last_service_time",
                        "is_busy": "queue_is_busy",
                    },
                },
            },
        },
    }

    # Run it
    start = time.time()
    env.run(until=1000000)
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
    print(df_dropped.shape)
    df_finished = df[df.end_time >= 0]
    print(df_finished.shape)
    df = df_finished

    df["end2end_delay"] = df["end_time"] - df["start_time"]
    df["service_delay"] = df["end_time"] - df["service_time"]
    df["queue_delay"] = df["service_time"] - df["queue_time"]
    df["time_in_service"] = df.apply(
        lambda row: (row.start_time - row.last_service_time)
        if row.queue_is_busy
        else None,
        axis=1,
    )

    del df["end_time"]
    del df["start_time"]
    del df["last_service_duration"]
    del df["last_service_time"]
    del df["queue_is_busy"]
    del df["service_time"]
    del df["queue_time"]

    print(df)

    df.to_parquet("dataset_onehop.parquet")

    # plot end-to-end delay profile
    sns.set_style("darkgrid")
    sns.displot(df["end2end_delay"], kde=True)
    plt.savefig("end2end.png")

    sns.displot(df["service_delay"], kde=True)
    plt.savefig("service_delay.png")

    sns.displot(df["queue_delay"], kde=True)
    plt.savefig("queue_delay.png")

    sns.displot(df["time_in_service"], kde=True)
    plt.savefig("time_in_service.png")

    sns.pairplot(
        data=df[["queue_length", "time_in_service"]],
        kind="kde",
        corner=True,
    )
    plt.savefig("pairplot.png")

    print(df["end2end_delay"].describe(percentiles=[0.9, 0.99, 0.999, 0.9999, 0.99999]))
    print(df["service_delay"].describe(percentiles=[0.9, 0.99, 0.999, 0.9999, 0.99999]))
    print(df["queue_delay"].describe(percentiles=[0.9, 0.99, 0.999, 0.9999, 0.99999]))
    print(
        df["time_in_service"].describe(percentiles=[0.9, 0.99, 0.999, 0.9999, 0.99999])
    )
