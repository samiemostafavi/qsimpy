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

    np.random.seed(0)

    arrival = functools.partial(random.uniform, 4.1, 4.1)

    # Gamma distributions, mean: 4
    service1 = functools.partial(np.random.gamma, 4, 1)
    service2 = functools.partial(np.random.gamma, 4, 1)
    service3 = functools.partial(np.random.gamma, 4, 1)


    # Create the QSimPy environment
    # a class for keeping all of the entities and accessing their attributes
    env = qsimpy.Environment(name='0')

    # Create the start-node and end-node
    source = qsimpy.Source(
                        name='start-node',
                        env=env, 
                        arrival_dist=arrival,
                        task_type='0',
    )

    queue1 = qsimpy.SimpleQueue(
                name='queue1',
                env=env,
                service_dist=service1,
                queue_limit=20,
    )

    queue2 = qsimpy.SimpleQueue(
                name='queue2',
                env=env,
                service_dist=service2,
                queue_limit=20,
    )

    queue3 = qsimpy.SimpleQueue(
                name='queue3',
                env=env,
                service_dist=service3,
                queue_limit=20,
    )

    sink = qsimpy.Sink(
                    name='sink',
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
        'timestamps' : {
            source.name : {
                'task_generation':'start_time',
            },
            queue1.name : {
                'task_reception':'queue1_queue_time',
                'service_start':'queue1_service_time',
            },
            queue2.name : {
                'task_reception':'queue2_queue_time',
                'service_start':'queue2_service_time',
            },
            queue3.name : {
                'task_reception':'queue3_queue_time',
                'service_start':'queue3_service_time',
                'service_end':'end_time',
            },
        },
        'attributes' : {
            source.name : {
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

    print("Source generated {0} tasks".format(source.get_attribute('tasks_generated')))
    print("Queue 1 completed {0}, dropped {1}".format(queue1.get_attribute('tasks_completed'),queue1.get_attribute('tasks_dropped')))
    print("Queue 2 completed {0}, dropped {1}".format(queue2.get_attribute('tasks_completed'),queue2.get_attribute('tasks_dropped')))
    print("Queue 3 completed {0}, dropped {1}".format(queue3.get_attribute('tasks_completed'),queue3.get_attribute('tasks_dropped')))
    print("Sink received {0} tasks".format(sink.get_attribute('tasks_received')))

    # Process the collected data
    df = pd.DataFrame(sink.received_tasks)
    df_dropped = df[df.end_time==-1]
    print(df_dropped.shape)
    df_finished = df[df.end_time>=0]
    print(df_finished.shape)
    df = df_finished

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

    