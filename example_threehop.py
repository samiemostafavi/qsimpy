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

    # service1 = functools.partial(random.expovariate, 1)
    # service2 = functools.partial(random.expovariate, 1)
    # service3 = functools.partial(random.expovariate, 1)

    arrival_rate = 0.09
    rng_arrival = np.random.default_rng(100234)
    arrival = functools.partial(
        rng_arrival.uniform, 1.00 / arrival_rate, 1.00 / arrival_rate
    )

    # Gamma distribution
    avg_service_rate = 0.1

    rng_service1 = np.random.default_rng(120034)
    service1 = functools.partial(rng_service1.gamma, 1.00 / avg_service_rate, 1)

    rng_service2 = np.random.default_rng(123004)
    service2 = functools.partial(rng_service2.gamma, 1.00 / avg_service_rate, 1)

    rng_service3 = np.random.default_rng(123400)
    service3 = functools.partial(rng_service3.gamma, 1.00 / avg_service_rate, 1)

    # Create the QSimPy environment
    # a class for keeping all of the entities and accessing their attributes
    env = qsimpy.Environment(name="0")

    # Create the start-node and end-node
    source = qsimpy.Source(
        name="start-node",
        env=env,
        arrival_dist=arrival,
        task_type="0",
    )

    queue1 = qsimpy.SimpleQueue(
        name="queue1",
        env=env,
        service_dist=service1,
        queue_limit=None,
    )

    queue2 = qsimpy.SimpleQueue(
        name="queue2",
        env=env,
        service_dist=service2,
        queue_limit=None,
    )

    queue3 = qsimpy.SimpleQueue(
        name="queue3",
        env=env,
        service_dist=service3,
        queue_limit=None,
    )

    sink = qsimpy.Sink(
        name="sink",
        env=env,
        debug=False,
    )

    # Wire source, queues, and sink together
    source.out = queue1
    queue1.out = queue2
    queue1.drop = sink
    queue2.out = queue3
    queue2.drop = sink
    queue3.out = sink
    queue3.drop = sink

    # records to save on the tasks during the run
    env.task_records = {
        "timestamps": {
            source.name: {
                "task_generation": "start_time",
            },
            queue1.name: {
                "task_reception": "queue1_queue_time",
                "service_start": "queue1_service_time",
            },
            queue2.name: {
                "task_reception": "queue2_queue_time",
                "service_start": "queue2_service_time",
            },
            queue3.name: {
                "task_reception": "queue3_queue_time",
                "service_start": "queue3_service_time",
                "service_end": "end_time",
            },
        },
        "attributes": {
            source.name: {
                "task_generation": {
                    queue1.name: {
                        "queue_length": "queue_length1",
                    },
                    queue2.name: {
                        "queue_length": "queue_length2",
                    },
                    queue3.name: {
                        "queue_length": "queue_length3",
                    },
                },
            },
        },
    }

    # Run it
    start = time.time()
    env.run(until=10000000)
    end = time.time()
    print("Run finished in {0} seconds".format(end - start))

    print("Source generated {0} tasks".format(source.get_attribute("tasks_generated")))
    print(
        "Queue 1 completed {0}, dropped {1}".format(
            queue1.get_attribute("tasks_completed"),
            queue1.get_attribute("tasks_dropped"),
        )
    )
    print(
        "Queue 2 completed {0}, dropped {1}".format(
            queue2.get_attribute("tasks_completed"),
            queue2.get_attribute("tasks_dropped"),
        )
    )
    print(
        "Queue 3 completed {0}, dropped {1}".format(
            queue3.get_attribute("tasks_completed"),
            queue3.get_attribute("tasks_dropped"),
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

    df["queue1_queue_delay"] = df["queue1_service_time"] - df["queue1_queue_time"]
    df["queue1_service_delay"] = df["queue2_queue_time"] - df["queue1_service_time"]

    df["queue2_queue_delay"] = df["queue2_service_time"] - df["queue2_queue_time"]
    df["queue2_service_delay"] = df["queue3_queue_time"] - df["queue2_service_time"]

    df["queue3_queue_delay"] = df["queue3_service_time"] - df["queue3_queue_time"]
    df["queue3_service_delay"] = df["end_time"] - df["queue3_service_time"]

    del df["end_time"]
    del df["start_time"]
    del df["queue1_queue_time"]
    del df["queue1_service_time"]
    del df["queue2_queue_time"]
    del df["queue2_service_time"]
    del df["queue3_queue_time"]
    del df["queue3_service_time"]

    print(df)

    df.to_parquet("dataset_threehop.parquet")

    # plot end-to-end delay profile
    sns.set_style("darkgrid")
    sns.displot(df["end2end_delay"], kde=True)
    plt.savefig("end2end.png")

    sns.pairplot(
        data=df[["queue_length1", "queue_length2", "queue_length3"]],
        kind="kde",
        corner=True,
    )
    plt.savefig("pairplot.png")

    print(df["end2end_delay"].describe(percentiles=[0.9, 0.99, 0.999, 0.9999, 0.99999]))

    # find 10 most common queue_length occurrences
    n = 9
    n1 = 3
    n2 = 3
    values_count = (
        df[["queue_length1", "queue_length2", "queue_length3"]]
        .value_counts()[:n]
        .index.tolist()
    )
    print("{0} most common queue states: {1}".format(n, values_count))
    # values_count = [(1,1,1),(0,0,0),(0,5,0),(0,10,0)]

    # plot the conditional distributions of them
    fig, axes = plt.subplots(ncols=n1, nrows=n2)

    for i, ax in zip(range(n), axes.flat):
        conditional_df = df[
            (df.queue_length1 == values_count[i][0])
            & (df.queue_length2 == values_count[i][1])
            & (df.queue_length3 == values_count[i][2])
        ]
        sns.histplot(conditional_df["end2end_delay"], kde=True, ax=ax).set(
            title="x={0}, count={1}".format(values_count[i], len(conditional_df))
        )
        ax.title.set_size(10)

    fig.tight_layout()
    plt.savefig("conditional_delay.png")
