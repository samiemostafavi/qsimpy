import pandas as pd
import qsimpy
import seaborn as sns
import matplotlib.pyplot as plt
import time

from qsimpy.random import Deterministic, Gamma

if __name__ == "__main__":

    # Create the QSimPy environment
    # a class for keeping all of the entities and accessing their attributes
    model = qsimpy.Model(name='test model')

    # arrival process uniform
    arrival = Deterministic(
        rate = 0.095,
        seed = 100234,
        dtype = 'float64',
    )

    # Create a source
    source = qsimpy.Source(
        name='start-node',
        arrival_rp=arrival,
        task_type='0',
    )
    model.add_entity(source)

    # service process a Gamma distribution
    avg_service_rate = 0.10
    service = Gamma(
        shape = 1.00/avg_service_rate, 
        scale = 1.00,
        seed  = 120034,
        dtype = 'float64'
    )

    # a queue
    queue = qsimpy.SimpleQueue(
        name='queue',
        service_rp= service,
        queue_limit=None,
    )
    model.add_entity(queue)

    # a node
    node = qsimpy.Node(
        name='node'
    )
    model.add_entity(node)

    # a sink: to capture both finished tasks and dropped tasks
    sink = qsimpy.Sink(
        name='sink',
    )
    model.add_entity(sink)

    # add task records
    model.set_task_records({
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
    )

    # setup routings
    source.out = queue.name
    queue.out = node.name
    queue.drop = sink.name
    node.out = sink.name
    
    model.prepare_for_run(debug=False)

    mstr = model.json()

    del model
    del source
    del queue
    del sink

    print("All components deleted, saved into JSON, and loaded:")

    new_model = qsimpy.Model.parse_raw(mstr)
    print(f"The model: {new_model.json()}")
    source = new_model.entities['start-node']
    queue = new_model.entities['queue']
    node = new_model.entities['node']
    sink = new_model.entities['sink']

    # Run it
    print("Run the model:")
    start = time.time()
    new_model.prepare_for_run(debug=False, clean=True)
    new_model.env.run(until=1000000)
    end = time.time()
    print("Run finished in {0} seconds".format(end - start))

    print("Source generated {0} tasks".format(source.get_attribute('tasks_generated')))
    print("Queue completed {0}, dropped {1}".format(
            queue.get_attribute('tasks_completed'),
            queue.get_attribute('tasks_dropped'),
        )
    )
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
                                lambda row: (row.start_time-row.last_service_time) if row.queue_is_busy else None,
                                axis=1,
                            )
    #df['time_in_service'] = df.apply(
    #                            lambda row: (row.last_service_duration - (row.start_time-row.last_service_time)) if row.queue_is_busy else None,
    #                            axis=1,
    #                        )

    del df['end_time']
    del df['start_time']
    del df['last_service_duration']
    del df['last_service_time']
    del df['queue_is_busy']
    del df['service_time']
    del df['queue_time']

    print(df)

    exit(0)

    df.to_parquet('dataset_onehop.parquet')

    # plot end-to-end delay profile
    sns.set_style('darkgrid')
    sns.displot(df['end2end_delay'],kde=True)
    plt.savefig('end2end.png')

    sns.displot(df['service_delay'],kde=True)
    plt.savefig('service_delay.png')

    sns.displot(df['queue_delay'],kde=True)
    plt.savefig('queue_delay.png')

    sns.displot(df['time_in_service'],kde=True)
    plt.savefig('time_in_service.png')

    sns.pairplot(
        data=df[['queue_length','time_in_service']],
        kind="kde",
        corner=True,
    )
    plt.savefig('pairplot.png')

    print(df['end2end_delay'].describe(percentiles=[0.9,0.99,0.999,0.9999,0.99999]))
    print(df['service_delay'].describe(percentiles=[0.9,0.99,0.999,0.9999,0.99999]))
    print(df['queue_delay'].describe(percentiles=[0.9,0.99,0.999,0.9999,0.99999]))
    print(df['time_in_service'].describe(percentiles=[0.9,0.99,0.999,0.9999,0.99999]))

