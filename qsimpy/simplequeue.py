from __future__ import annotations

import simpy
from typing import Dict, Callable

from .core import Task, Entity, Environment


class SimpleQueue(Entity):
    """ Models a FIFO queue with a service delay process and buffer size limit in number of tasks.
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
        super().__init__(name,env,attributes,events)

    def run(self):
        """
        serving tasks
        """
        while True:
            
            #server takes the head task from the queue
            task = (yield self.store.get())
            self.attributes['queue_length'] -= 1

            # task_service event records
            task = self.add_records(task=task, event_name='task_service')

            # get a service duration 
            self.attributes['is_busy'] = True
            new_service_duration = self.service_dist()
            self.attributes['last_service_duration'] = new_service_duration
            self.attributes['last_service_time'] = self.env.now

            # wait until the task is served
            yield self.env.timeout(new_service_duration)

            # put it on the output
            self.out.put(task)
            self.attributes['is_busy'] = False

            if self.debug:
                print(task)

    def put(self, 
            task: Task
        ):
        """
        queuing tasks
        """

        # increase the received counter
        self.attributes['tasks_received'] += 1

        # check if we need to drop the task due to buffer size limit
        drop = False
        if self.queue_limit is not None:       
            if self.attributes['queue_length']+1 >= self.queue_limit:
                self.attributes['tasks_dropped'] += 1
                drop = True

        if not drop:
            # task_reception event records
            task = self.add_records(task=task, event_name='task_reception')

            # store the task in the queue
            self.attributes['queue_length'] += 1
            return self.store.put(task)
