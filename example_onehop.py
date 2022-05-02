import random
import functools
import pandas as pd
import qsimpy
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

if __name__ == "__main__":

    #arrival = functools.partial(random.expovariate, 0.8)
    #arrival = functools.partial(random.uniform, 1.1, 1.1)
    #service = functools.partial(random.expovariate, 1)

    arrival = functools.partial(random.uniform, 4.1, 4.1)

    # Gamma distributions, mean: 4
    service = functools.partial(np.random.gamma, 4, 1)

    # Create the QSimPy environment
    # a class for keeping all of the entities and accessing their attributes
    env = qsimpy.Environment(name='0')

    # Create a source
    source = qsimpy.Source(
                        name='start-node',
                        env=env,
                        arrival_dist=arrival,
                        task_type='0',
    )

    # a queue
    queue = qsimpy.SimpleQueue(
                name='queue',
                env=env,
                service_dist=service,
                queue_limit=20
    )

    # a sink: to capture both finished tasks and dropped tasks
    sink = qsimpy.Sink(
                    name='sink',
                    env=env,
                    debug=False,
    )

    # Wire start-node, queue, end-node, and sink together
    source.out = queue
    queue.out = sink
    queue.drop = sink

    env.task_records = {
        'timestamps' : {
            source.name : {
                'task_generation':'start_time',
            },
            queue.name : {
                'task_reception':'queue_time',
                'service_start':'service_time',
                'service_end':'end_time',
            },
        },
        'attributes' : {
            source.name : {
                'task_generation' : {
                    queue.name : {
                        'queue_length':'queue_length',
                        'last_service_duration':'last_service_duration',
                        'last_service_time':'last_service_time',
                        'is_busy':'queue_is_busy',
                    },
                },
            },
        },
    }

    # Run it
    env.run(until=100000)

    print("Source generated {0} tasks".format(source.get_attribute('tasks_generated')))
    print("Queue completed {0}, dropped {1}".format(queue.get_attribute('tasks_completed'),queue.get_attribute('tasks_dropped')))
    print("Sink received {0} tasks".format(sink.get_attribute('tasks_received')))

    # Process the collected data
    df = pd.DataFrame(sink.received_tasks)

    df_dropped = df[df.end_time==-1]
    print(df_dropped.shape)
    df_finished = df[df.end_time>=0]
    print(df_finished.shape)
    df = df_finished

    df['end2end_delay'] = df['end_time']-df['start_time']
    df['service_delay'] = df['end_time']-df['service_time']
    df['queue_delay'] = df['service_time']-df['queue_time']
    df['time_in_service'] = df.apply(
                                lambda row: (row.last_service_duration - (row.start_time-row.last_service_time)) if row.queue_is_busy else 0,
                                axis=1,
                            )

    del df['end_time']
    del df['start_time']
    del df['last_service_duration']
    del df['last_service_time']
    del df['queue_is_busy']
    del df['service_time']
    del df['queue_time']

    print(df)

    sns.set_style('darkgrid')
    sns.displot(df['end2end_delay'],kde=True)
    sns.displot(df['service_delay'],kde=True)
    sns.displot(df['queue_delay'],kde=True)

    print(df['end2end_delay'].describe(percentiles=[0.9,0.99,0.999,0.9999,0.99999]))
    print(df['service_delay'].describe(percentiles=[0.9,0.99,0.999,0.9999,0.99999]))
    print(df['queue_delay'].describe(percentiles=[0.9,0.99,0.999,0.9999,0.99999]))

    plt.show()

    