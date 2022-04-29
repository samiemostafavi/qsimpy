import simpy
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

    arrival = functools.partial(random.uniform, 4.4, 4.4)

    # Gamma distributions, mean: 4
    service = functools.partial(np.random.gamma, 4, 1)

    # Create the QSimPy environment
    # a class for keeping all of the entities and accessing their attributes
    env = qsimpy.Environment(name='0')

    # Create the start-node and end-node
    startnode = qsimpy.StartNode(
                        name='start-node',
                        env=env,
                        arrival_dist=arrival)

    queue = qsimpy.SimpleQueue(
                name='queue',
                env=env,
                service_dist=service,
                queue_limit=1000)

    endnode = qsimpy.EndNode(
                    name='end-node',
                    env=env,
                    debug=False)

    # Wire start-node, queue, and end-node together
    startnode.out = queue
    queue.out = endnode

    env.task_records = {
        'timestamps' : {
            startnode.name : {
                'task_generation':'start_time',
            },
            queue.name : {
                'task_reception':'queue_time',
                'task_service':'service_time',
            },
            endnode.name : {
                'task_reception':'end_time',
            }
        },
        'attributes' : {
            startnode.name : {
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

    # Process the collected data
    df = pd.DataFrame(endnode.received_tasks)
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

    