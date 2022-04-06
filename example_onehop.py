import simpy
import random
import functools
import pandas as pd

from qsimpy import *


if __name__ == "__main__":

    #arrival = functools.partial(random.expovariate, 0.8)
    arrival = functools.partial(random.uniform, 1.1, 1.1)
    service = functools.partial(random.expovariate, 1)

    env = simpy.Environment()  # Create the SimPy environment
    flow = Flow(name='0')

    records_config = {
        'timestamps' : {
            'start-node' : {
                'task_generation':'start_time',
            },
            'queue' : {
                'task_reception':'queue_time',
                'task_service':'service_time',
            },
            'end-node' : {
                'task_reception':'end_time',
            }
        },
        'attributes' : {
            'start-node' : {
                'task_generation' : {
                    'queue' : {
                        'queue_length':'queue_length',
                        'last_service_duration':'last_service_duration',
                        'last_service_time':'last_service_time',
                        'is_busy':'queue_is_busy',
                    },
                },
            },
        },
    }

    # Create the start-node and end-node
    startnode = StartNode(
                        name='start-node',
                        env=env, 
                        flow=flow,
                        arrival_dist=arrival,
                        records_config=records_config)

    queue = SimpleQueue(
                name='queue',
                env=env,
                flow=flow,
                service_dist=service,
                queue_limit=1000,
                records_config=records_config)

    endnode = EndNode(
                    name='end-node',
                    env=env,
                    flow=flow,
                    debug=False,
                    records_config=records_config)

    # Wire start-node, queue, and end-node together
    startnode.out = queue
    queue.out = endnode

    # Run it
    env.run(until=10000)

    # Process the collected data
    df = pd.DataFrame(endnode.received_tasks)
    df['end2end_delay'] = df['end_time']-df['start_time']
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