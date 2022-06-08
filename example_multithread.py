import functools
import pandas as pd
import qsimpy
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import multiprocessing as mp
import time


def create_run_graph(params):
    # params = {
    #   'run_number' : 0,
    #   'arrival_seed' : 100234,
    #   'service_seed' : 120034,
    # }

    # arrival function: Uniform
    arrival_rate = 0.095
    rng_arrival = np.random.default_rng(params['arrival_seed'])
    arrival = functools.partial(rng_arrival.uniform, 1.00/arrival_rate, 1.00/arrival_rate)

    # service function: Gamma distribution
    avg_service_rate = 0.10
    rng_service = np.random.default_rng(params['service_seed'])
    service = functools.partial(rng_service.gamma, 1.00/avg_service_rate, 1)

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
        queue_limit=None,
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
                        'last_service_time':'last_service_time',
                        'is_busy':'queue_is_busy',
                    },
                },
            },
        },
    }

    # Run it
    start = time.time()
    env.run(until=params['until'])
    end = time.time()

    print("{0}: Run finished in {1} seconds".format(params['run_number'],end - start))

    start = time.time()

    # Process the collected data
    df = pd.DataFrame(sink.received_tasks)

    df_dropped = df[df.end_time==-1]
    df_finished = df[df.end_time>=0]
    df = df_finished

    df['end2end_delay'] = df['end_time']-df['start_time']
    df['service_delay'] = df['end_time']-df['service_time']
    df['queue_delay'] = df['service_time']-df['queue_time']
    df['time_in_service'] = df.apply(
                                lambda row: (row.start_time-row.last_service_time) if row.queue_is_busy else None,
                                axis=1,
                            )

    df.drop([
            'end_time',
            'start_time',
            'last_service_time',
            'queue_is_busy',
            'service_time',
            'queue_time',
        ], 
        axis = 1,
        inplace = True,
    )

    end = time.time()

    df.to_parquet('dataset_{}_onehop.parquet'.format(params['run_number']))

    print("{0}: Data processing finished in {1} seconds".format(params['run_number'],end - start))

    

if __name__ == "__main__":


    processes = []
    for i in range(2):
        params = {
            'run_number' : i,
            'arrival_seed' : 100234+i*100101,
            'service_seed' : 120034+i*200202,
            'until': 1000000,
        }
        p = mp.Process(target=create_run_graph, args=(params,))
        p.start()
        processes.append(p)
    

    for p in processes:
        p.join()

