from __future__ import annotations

import simpy
from typing import Dict, Callable

from .basic import Task, Entity, Environment


class SimpleQueue(Entity):
    """ Models a queue with a service delay process and buffer size limit in number of tasks.
        Set the "out" member variable to the entity to receive the task.
        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        service_dist : function
            a no parameter function that returns the successive service times of the tasks
        queue_limit : integer (or None)
            a buffer size limit in number of tasks for the queue (does not include the task in service).
    """
    def __init__(self,
                name : str,
                env : Environment, 
                service_dist : Callable,
                records_config: Dict,
                queue_limit: int=None, 
                debug: bool=False,
            ):

        self.store = simpy.Store(env)
        self.service_dist = service_dist
        self.out = None
        self.queue_limit = queue_limit
        self.debug = debug

        # initialize the attributes
        attributes = {
            'tasks_received':0,
            'tasks_dropped':0,
            'queue_length':0,
            'last_service_duration':0,
            'last_service_time':0,
            'is_busy':False,
        }

        # advertize the events
        events = { 'task_reception', 'task_service' }

        # initialize the entity
        super().__init__(name,env,attributes,events,records_config)

    def run(self):
        while True:
            task = (yield self.store.get())

            # add event records
            task = self.add_records(task=task, event_name='task_service')

            self.attributes['is_busy'] = True
            self.attributes['queue_length'] -= 1
            new_service_duration = self.service_dist()
            self.attributes['last_service_duration'] = new_service_duration
            self.attributes['last_service_time'] = self.env.now
            yield self.env.timeout(new_service_duration)

            self.out.put(task)
            self.attributes['is_busy'] = False
            if self.debug:
                print(task)

    def put(self, 
            task: Task
        ):

        self.attributes['tasks_received'] += 1
        tmp = self.attributes['queue_length'] + 1

        drop = False

        if self.queue_limit is not None:       
            if tmp >= self.queue_limit:
                self.attributes['tasks_dropped'] += 1
                drop = True

        if not drop:
            # add event records
            task = self.add_records(task=task, event_name='task_reception')

            self.attributes['queue_length'] = tmp
            return self.store.put(task)
