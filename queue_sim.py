from __future__ import annotations
from email.policy import default

import simpy
import random
import copy
from simpy.core import BoundClass
from simpy.resources import base
from heapq import heappush, heappop
import random
import functools
import pandas as pd
from dataclasses import dataclass,make_dataclass,field
from typing import Dict,FrozenSet,Any


@dataclass(frozen=False, eq=True)
class Task():
    """ A very simple dataclass that represents a task.
        This task will run through the tandem queues.
        We use a float to represent the size of the packet in bytes

        Parameters
        ----------
        id : int
            an identifier for the task
        flow_name : str
            string that can be used to identify a flow
        start_time : float
            when the task leaves the start-node
        end_time : float
            when the task reaches the end-node
        queue_time : float
            waiting time of the task in the queue
        service_time : float
            service time of the task
        queue_length : int
            the queue's system length when the task is generated
        queue_time_in_service : float
            time_in_service of the queue when the task is generated
    """
    id: int
    flow_name: str


def get_all_values(d):
    if isinstance(d, dict):
        for v in d.values():
            yield from get_all_values(v)
    elif isinstance(d, list):
        for v in d:
            yield from get_all_values(v)
    else:
        yield d 


class Flow():
    """ A singletone class for keeping the entities and accessing 
        their attributes
    """
    name : str
    entities : Dict[str,Entity]
    def __init__(self, name):
        self.name = name
        self.entities = {}


class Entity():
    name : str
    env : simpy.Environment
    flow : Flow
    attributes : Dict[str,Any]
    events : FrozenSet[str]
    def __init__(self,
                name : str,
                env : simpy.Environment,
                flow : Flow,
                attributes : Dict[str,Any],
                events : FrozenSet[str],
                records_config: Dict,
    ):
        self.name = name
        self.env = env
        self.flow = flow
        self.attributes = attributes
        self.events = events
        self.records_config = records_config

        # assign the run function
        self.action = env.process(self.run())  # starts the run() method as a SimPy process

        # assign the flow
        self.flow.entities[self.name] = self

    def get_attribute(self,
                    name: str,
                ) -> Any:
        return self.attributes[name]
    def get_all_attributes(self) -> Dict[str,Any]:
        return self.attributes
    def get_events_names(self) -> FrozenSet[str]:
        return self.events
    def run(self) -> None:
        pass
    def add_records(self,
                    task: Task,
                    event_name: str,
                ) -> Task:

        # task_generation event add the timestamp
        ts_dict = self.records_config.get('timestamps',{}).get(self.name,{})
        if event_name in records_config.get('timestamps',{}).get(self.name,{}):
            task.__setattr__(ts_dict[event_name],self.env.now)

        # task_generation event attributes record
        att_dict = self.records_config.get('attributes',{}).get(self.name,{}).get(event_name,{})
        for entity_name in att_dict:
            for attribute in att_dict[entity_name]:
                value = self.flow.entities[entity_name].get_attribute(attribute)
                task.__setattr__(att_dict[entity_name][attribute],value)

        return task


class StartNode(Entity):
    """ Generates tasks with given inter-arrival time distribution.
        Set the "out" member variable to the entity to receive the task.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        arrival_dist : function
            a no parameter function that returns the successive inter-arrival times of the tasks
        initial_delay : number
            Starts task generation after an initial delay. Default = 0
        finish_time : number
            Stops generation at the finish time. Default is infinite
    """
    def __init__(self,
                name : str,
                env : simpy.Environment,
                flow : Flow,
                records_config: Dict,
                arrival_dist,
                initial_delay : float=0, 
                finish_time : float = float("inf"), 
            ):
        
        # initialization
        self.arrival_dist = arrival_dist
        self.initial_delay = initial_delay
        self.finish = finish_time
        self.out = None
        
        # initialize the attributes
        attributes = {
            'tasks_generated':0,
        }

        # advertize the events
        events = { 'task_generation' }

        # initialize the entity
        super().__init__(name,env,flow,attributes,events,records_config)

    def generate_task(self):
        new_task = Task(
            id=self.attributes['tasks_generated'], 
            flow_name=self.flow.name
        )
        fields = [ (name, float, field(default=-1)) for name in get_all_values(self.records_config) ]
        new_task.__class__ = make_dataclass('GeneratedTask', fields=fields, bases=(Task,))
        return new_task

    def run(self):
        """The generator function used in simulations.
        """
        yield self.env.timeout(self.initial_delay)
        while self.env.now < self.finish:
            # wait for next transmission
            yield self.env.timeout(self.arrival_dist())
            new_task = self.generate_task()
            self.attributes['tasks_generated'] += 1
            
            # add event records
            new_task = self.add_records(task=new_task, event_name='task_generation')

            if self.out is not None:
                self.out.put(new_task)



class EndNode(Entity):
    """ The end-node that receives tasks and collects delay information.
        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        debug : boolean
            if true then the contents of each task will be printed as it is received.
    """
    def __init__(self,
                name : str,
                env : simpy.Environment, 
                flow : Flow,
                records_config: Dict,
                debug=False,
            ):

        self.store = simpy.Store(env)
        self.debug = debug

        # initialize the attributes
        attributes = {
            'tasks_received':0,
        }

        # advertize the events
        events = { 'task_reception' }

        # initialize the entity
        super().__init__(name,env,flow,attributes,events,records_config)

    def run(self):
        while True:
            task = (yield self.store.get())

            # add event records
            task = self.add_records(task=task, event_name='task_reception')

            self.attributes['tasks_received'] += 1
            if self.debug:
                print(task)

    def put(self, task):
        self.store.put(task)


class Queue(Entity):
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
                env : simpy.Environment, 
                flow : Flow,
                service_dist,
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
        super().__init__(name,env,flow,attributes,events,records_config)

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
                        'is_busy':'is_busy',
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

    queue = Queue(
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
                    debug=True,
                    records_config=records_config)

    # Wire start-node, queue, and end-node together
    startnode.out = queue
    queue.out = endnode


    # Run it
    env.run(until=8000)