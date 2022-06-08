import functools
import pandas as pd
import qsimpy
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import time

if __name__ == "__main__":

    #arrival = functools.partial(random.expovariate, 0.8)
    #arrival = functools.partial(random.uniform, 1.1, 1.1)

    #service1 = functools.partial(random.expovariate, 1)
    #service2 = functools.partial(random.expovariate, 1)
    #service3 = functools.partial(random.expovariate, 1)

    arrival_rate = 0.09
    rng_arrival = np.random.default_rng(100234)
    arrival = functools.partial(rng_arrival.uniform, 1.00/arrival_rate, 1.00/arrival_rate)

    # Gamma distribution
    avg_service_rate = 0.1
    
    rng_service1 = np.random.default_rng(120034)
    service1 = functools.partial(rng_service1.gamma, 1.00/avg_service_rate, 1)

    rng_service2 = np.random.default_rng(123004)
    service2 = functools.partial(rng_service2.gamma, 1.00/avg_service_rate, 1)

    rng_service3 = np.random.default_rng(123400)
    service3 = functools.partial(rng_service3.gamma, 1.00/avg_service_rate, 1)


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
                queue_limit=None,
    )

    queue2 = qsimpy.SimpleQueue(
                name='queue2',
                env=env,
                service_dist=service2,
                queue_limit=None,
    )

    queue3 = qsimpy.SimpleQueue(
                name='queue3',
                env=env,
                service_dist=service3,
                queue_limit=None,
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
                        'queue_length':'a_queue_length1',
                        'last_service_time':'a_last_service_time1',
                        'is_busy':'a_queue_is_busy1',
                    },
                    queue2.name : {
                        'queue_length':'a_queue_length2',
                        'last_service_time':'a_last_service_time2',
                        'is_busy':'a_queue_is_busy2',
                    },
                    queue3.name : {
                        'queue_length':'a_queue_length3',
                        'last_service_time':'a_last_service_time3',
                        'is_busy':'a_queue_is_busy3',
                    },
                },
            },
            queue1.name : {
                'service_end' : {
                    queue2.name : {
                        'queue_length':'b_queue_length2',
                        'last_service_time':'b_last_service_time2',
                        'is_busy':'b_queue_is_busy2',
                    },
                    queue3.name : {
                        'queue_length':'b_queue_length3',
                        'last_service_time':'b_last_service_time3',
                        'is_busy':'b_queue_is_busy3',
                    },
                },
            },
            queue2.name : {
                'service_end' : {
                    queue3.name : {
                        'queue_length':'c_queue_length3',
                        'last_service_time':'c_last_service_time3',
                        'is_busy':'c_queue_is_busy3',
                    },
                },
            },
        },
    }


    # Run it
    start = time.time()
    env.run(until=100000)
    end = time.time()
    print("Run finished in {0} seconds".format(end - start))

    print("Source generated {0} tasks".format(source.get_attribute('tasks_generated')))
    print("Queue 1 completed {0}, dropped {1}".format(queue1.get_attribute('tasks_completed'),queue1.get_attribute('tasks_dropped')))
    print("Queue 2 completed {0}, dropped {1}".format(queue2.get_attribute('tasks_completed'),queue2.get_attribute('tasks_dropped')))
    print("Queue 3 completed {0}, dropped {1}".format(queue3.get_attribute('tasks_completed'),queue3.get_attribute('tasks_dropped')))
    print("Sink received {0} tasks".format(sink.get_attribute('tasks_received')))

    # Process the collected data

    # collect data from the sink node
    df = pd.DataFrame(sink.received_tasks)

    # seperate the dropped packets
    df_dropped = df[df.end_time==-1]
    print("Dropped packets: {}".format(df_dropped.shape[0]))
    df_finished = df[df.end_time>=0]
    print("Received packets: {}".format(df_finished.shape[0]))
    df = df_finished

    # general features
    df['end2end_delay'] = df['end_time']-df['start_time']
    df['queue1_queue_delay'] = df['queue1_service_time']-df['queue1_queue_time']
    df['queue1_service_delay'] = df['queue2_queue_time']-df['queue1_service_time']
    df['queue2_queue_delay'] = df['queue2_service_time']-df['queue2_queue_time']
    df['queue2_service_delay'] = df['queue3_queue_time']-df['queue2_service_time']
    df['queue3_queue_delay'] = df['queue3_service_time']-df['queue3_queue_time']
    df['queue3_service_delay'] = df['end_time']-df['queue3_service_time']

    # point 'a' features:
    # a_2end_delay, 
    # a_time_in_service1, a_time_in_service2, a_time_in_service3,
    # a_queue_length1, a_queue_length2, a_queue_length3
    df['a_2end_delay'] = df['end_time']-df['start_time']
    df['a_time_in_service1'] = df.apply(
        lambda row: (row.start_time-row.a_last_service_time1) if row.a_queue_is_busy1 else None,
        axis=1,
    )
    df['a_time_in_service2'] = df.apply(
        lambda row: (row.start_time-row.a_last_service_time2) if row.a_queue_is_busy2 else None,
        axis=1,
    )
    df['a_time_in_service3'] = df.apply(
        lambda row: (row.start_time-row.a_last_service_time3) if row.a_queue_is_busy3 else None,
        axis=1,
    )

    # point 'b' features:
    # b_2end_delay, 
    # b_time_in_service2, b_time_in_service3,
    # b_queue_length2, b_queue_length3
    df['b_2end_delay'] = df['end_time']-df['queue2_queue_time']
    df['b_time_in_service2'] = df.apply(
        lambda row: (row.queue2_queue_time-row.b_last_service_time2) if row.b_queue_is_busy2 else None,
        axis=1,
    )
    df['b_time_in_service3'] = df.apply(
        lambda row: (row.queue2_queue_time-row.b_last_service_time3) if row.b_queue_is_busy3 else None,
        axis=1,
    )

    # point 'c' features:
    # c_2end_delay, 
    # c_time_in_service3, 
    # c_queue_length3
    df['c_2end_delay'] = df['end_time']-df['queue3_queue_time']
    df['c_time_in_service3'] = df.apply(
        lambda row: (row.queue3_queue_time-row.c_last_service_time3) if row.c_queue_is_busy3 else None,
        axis=1,
    )

    df.drop([
            'end_time',
            'start_time',
            'queue1_queue_time',
            'queue1_service_time',
            'queue2_queue_time',
            'queue2_service_time',
            'queue3_queue_time',
            'queue3_service_time',
            'a_last_service_time1',
            'a_queue_is_busy1',
            'a_last_service_time2',
            'a_queue_is_busy2',
            'a_last_service_time3',
            'a_queue_is_busy3',
            'b_last_service_time2',
            'b_queue_is_busy2',
            'b_last_service_time3',
            'b_queue_is_busy3',
            'c_last_service_time3',
            'c_queue_is_busy3',
        ], 
        axis = 1,
        inplace = True,
    )

    print(df)

    df.to_parquet('dataset_threehop_tis.parquet')

    # plot end-to-end delay profile
    sns.set_style('darkgrid')
    sns.displot(df['end2end_delay'],kde=True)
    plt.savefig('a_2end_delay.png')

    sns.displot(df['b_2end_delay'],kde=True)
    plt.savefig('b_2end_delay.png')

    sns.displot(df['c_2end_delay'],kde=True)
    plt.savefig('c_2end_delay.png')

    #sns.pairplot(
    #    data=df[['queue_length1','queue_length2','queue_length3']],
    #    kind="kde",
    #    corner=True,
    #)
    #plt.savefig('pairplot.png')
    
    print(df['end2end_delay'].describe(percentiles=[0.9,0.99,0.999,0.9999,0.99999]))

    # find 10 most common queue_length occurances
    n = 4
    n1 = 2; n2= 2
    values_count = df[['a_queue_length1','a_queue_length2','a_queue_length3']].value_counts()[:n].index.tolist()
    print("{0} most common queue states: {1}".format(n,values_count))
    #values_count = [(1,1,1),(0,0,0),(0,5,0),(0,10,0)]

    # plot the conditional distributions of them
    fig, axes = plt.subplots(ncols=n1, nrows=n2)

    for i, ax in zip(range(n), axes.flat):
        conditional_df = df[
            (df.a_queue_length1==values_count[i][0]) & 
            (df.a_queue_length2==values_count[i][1]) & 
            (df.a_queue_length3==values_count[i][2])
        ]
        sns.histplot(
            conditional_df['end2end_delay'],
            kde=True, 
            ax=ax
        ).set(title="x={0}, count={1}".format(values_count[i],len(conditional_df)))
        ax.title.set_size(10)

    fig.tight_layout()
    plt.savefig('conditional_delay.png')

    