import pandas as pd
import qsimpy
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import time
import psutil as ps
import polars as pl
import os

from qsimpy.random import Deterministic, Gamma

# https://stackoverflow.com/questions/41240470/python-simpy-memory-usage-with-large-numbers-of-objects-processes


class MemoryUse(object):
    """a class used to output memory usage at various times within the sim"""

    def __init__(self, env):

        self.env = env

        self.env.process(self.before())
        self.env.process(self.during_1())
        self.env.process(self.during_2())
        self.env.process(self.during_3())

    def before(self):

        yield self.env.timeout(0)
        print(f"full object list and memory events at time: {self.env.now}, {ps.Process(os.getpid()).memory_info().rss / 1024 ** 2} MB")

    def during_1(self):
        yield self.env.timeout(300000)
        print(f"full object list and memory events at time: {self.env.now}, {ps.Process(os.getpid()).memory_info().rss / 1024 ** 2} MB")

    def during_2(self):
        yield self.env.timeout(600000)
        print(f"full object list and memory events at time: {self.env.now}, {ps.Process(os.getpid()).memory_info().rss / 1024 ** 2} MB")

    def during_3(self):
        yield self.env.timeout(990000)
        print(f"full object list and memory events at time: {self.env.now}, {ps.Process(os.getpid()).memory_info().rss / 1024 ** 2} MB")



if __name__ == "__main__":

    # memory usage before we do anything
    print("before all: ", ps.virtual_memory())

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


    # a sink: to capture both finished tasks and dropped tasks (compare PolarSink vs Sink)
    sink = qsimpy.PolarSink(
        name='sink',
        debug=False,
        batch_size = 10000,
    )
    # define postprocess function
    def process_time_in_service(df):
 
        df['end2end_delay'] = df['end_time']-df['start_time']
        df['service_delay'] = df['end_time']-df['service_time']
        df['queue_delay'] = df['service_time']-df['queue_time']

        df['time_in_service'] = df.apply(
                                lambda row: (row.start_time-row.last_service_time) if row.queue_is_busy else None,
                                axis=1,
                            ).astype('float64')

        del df['last_service_duration'], df['last_service_time'], df['queue_is_busy']

        return df

    sink.set_post_process_fn(fn=process_time_in_service)
    model.add_entity(sink)

    # Wire start-node, queue, end-node, and sink together
    source.out = queue.name
    queue.out = sink.name
    queue.drop = sink.name

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
    })

    # prepare for run
    model.prepare_for_run(debug=False)

    # memory usage before we do anything
    print("before start: ", ps.virtual_memory())

    # create memory calculation events
    memory = MemoryUse(model.env)

    # Run it
    start = time.time()
    model.env.run(until=1000000)
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
    df = sink.received_tasks
    print(df)
    df_dropped = df.filter(pl.col('end_time') == -1)
    print(df_dropped.shape)
    df_finished = df.filter(pl.col('end_time') >= 0)
    print(df_finished.shape)
    df = df_finished

    df.write_parquet('memorytest_onehop.parquet')


