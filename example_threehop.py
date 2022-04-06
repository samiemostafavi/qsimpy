import simpy
import random
import functools
import pandas as pd

from qsimpy import *


if __name__ == "__main__":

    #arrival = functools.partial(random.expovariate, 0.8)
    arrival = functools.partial(random.uniform, 1.1, 1.1)
    service1 = functools.partial(random.expovariate, 1)
    service2 = functools.partial(random.expovariate, 1)
    service3 = functools.partial(random.expovariate, 1)

    env = simpy.Environment()  # Create the SimPy environment
    flow = Flow(name='0')

    records_config = {
        'timestamps' : {
            'start-node' : {
                'task_generation':'start_time',
            },
            'queue1' : {
                'task_reception':'queue1_queue_time',
                'task_service':'queue1_service_time',
            },
            'queue2' : {
                'task_reception':'queue2_queue_time',
                'task_service':'queue2_service_time',
            },
            'queue3' : {
                'task_reception':'queue3_queue_time',
                'task_service':'queue3_service_time',
            },
            'end-node' : {
                'task_reception':'end_time',
            }
        },
        'attributes' : {
            'start-node' : {
                'task_generation' : {
                    'queue1' : {
                        'queue_length':'queue_length1',
                    },
                    'queue2' : {
                        'queue_length':'queue_length2',
                    },
                    'queue3' : {
                        'queue_length':'queue_length3',
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

    queue1 = SimpleQueue(
                name='queue1',
                env=env,
                flow=flow,
                service_dist=service1,
                queue_limit=1000,
                records_config=records_config)

    queue2 = SimpleQueue(
                name='queue2',
                env=env,
                flow=flow,
                service_dist=service2,
                queue_limit=1000,
                records_config=records_config)

    queue3 = SimpleQueue(
                name='queue3',
                env=env,
                flow=flow,
                service_dist=service3,
                queue_limit=1000,
                records_config=records_config)

    endnode = EndNode(
                    name='end-node',
                    env=env,
                    flow=flow,
                    debug=False,
                    records_config=records_config)

    # Wire start-node, queues, and end-node together
    startnode.out = queue1
    queue1.out = queue2
    queue2.out = queue3
    queue3.out = endnode

    # Run it
    env.run(until=10000)

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