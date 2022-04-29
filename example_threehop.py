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

    #service1 = functools.partial(random.expovariate, 1)
    #service2 = functools.partial(random.expovariate, 1)
    #service3 = functools.partial(random.expovariate, 1)

    arrival = functools.partial(random.uniform, 4.4, 4.4)

    # Gamma distributions, mean: 4
    service1 = functools.partial(np.random.gamma, 4, 1)
    service2 = functools.partial(np.random.gamma, 4, 1)
    service3 = functools.partial(np.random.gamma, 4, 1)
    

    # Create the QSimPy environment
    # a class for keeping all of the entities and accessing their attributes
    env = qsimpy.Environment(name='0')

    # Create the start-node and end-node
    startnode = qsimpy.StartNode(
                        name='start-node',
                        env=env, 
                        arrival_dist=arrival)

    queue1 = qsimpy.SimpleQueue(
                name='queue1',
                env=env,
                service_dist=service1,
                queue_limit=1000)

    queue2 = qsimpy.SimpleQueue(
                name='queue2',
                env=env,
                service_dist=service2,
                queue_limit=1000)

    queue3 = qsimpy.SimpleQueue(
                name='queue3',
                env=env,
                service_dist=service3,
                queue_limit=1000)

    endnode = qsimpy.EndNode(
                    name='end-node',
                    env=env,
                    debug=False)

    # Wire start-node, queues, and end-node together
    startnode.out = queue1
    queue1.out = queue2
    queue2.out = queue3
    queue3.out = endnode

    # records to save on the tasks during the run
    env.task_records = {
        'timestamps' : {
            startnode.name : {
                'task_generation':'start_time',
            },
            queue1.name : {
                'task_reception':'queue1_queue_time',
                'task_service':'queue1_service_time',
            },
            queue2.name : {
                'task_reception':'queue2_queue_time',
                'task_service':'queue2_service_time',
            },
            queue3.name : {
                'task_reception':'queue3_queue_time',
                'task_service':'queue3_service_time',
            },
            endnode.name : {
                'task_reception':'end_time',
            }
        },
        'attributes' : {
            startnode.name : {
                'task_generation' : {
                    queue1.name : {
                        'queue_length':'queue_length1',
                    },
                    queue2.name : {
                        'queue_length':'queue_length2',
                    },
                    queue3.name : {
                        'queue_length':'queue_length3',
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

    df['queue1_queue_delay'] = df['queue1_service_time']-df['queue1_queue_time']
    df['queue1_service_delay'] = df['queue2_queue_time']-df['queue1_service_time']

    df['queue2_queue_delay'] = df['queue2_service_time']-df['queue2_queue_time']
    df['queue2_service_delay'] = df['queue3_queue_time']-df['queue2_service_time']

    df['queue3_queue_delay'] = df['queue3_service_time']-df['queue3_queue_time']
    df['queue3_service_delay'] = df['end_time']-df['queue3_service_time']

    del df['end_time']
    del df['start_time']
    del df['queue1_queue_time']
    del df['queue1_service_time']
    del df['queue2_queue_time']
    del df['queue2_service_time']
    del df['queue3_queue_time']
    del df['queue3_service_time']

    print(df)

    sns.set_style('darkgrid')
    sns.displot(df['end2end_delay'],kde=True)

    print(df['end2end_delay'].describe(percentiles=[0.9,0.99,0.999,0.9999,0.99999]))

    plt.show()

    