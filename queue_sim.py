import simpy
import random
import copy
from simpy.core import BoundClass
from simpy.resources import base
from heapq import heappush, heappop
import random
import functools
import simpy
import matplotlib.pyplot as plt
import pandas as pd
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=False, eq=True)
class Entity():
    name : str
    id : int
    def get_attribute(self,
                    name: str,
                ):
        pass


class Flow():
    """ A singletone class for keeping the entities and accessing 
        their attributes
    """
    id : int
    entities : Dict[str,Entity]
    def __init__(self, id):
        self.id = id
        self.entities = {}



@dataclass(frozen=False, eq=True)
class Task():
    """ A very simple dataclass that represents a task.
        This task will run through the tandem queues.
        We use a float to represent the size of the packet in bytes

        Parameters
        ----------
        id : int
            an identifier for the task
        flow_id : int
            small integer that can be used to identify a flow
        start_time : float
            when the task leaves the start-node
        end_time : float
            when the task reaches the end-node
        queue_length : int
            the queue length when the task is generated
        queue_time : float
            waiting time of the task in the queue
        service_time : float
            service time of the task
    """

    id: int
    flow_id: int
    start_time: float
    end_time: float = None
    queue_length: int = None
    queue_time: float = None
    service_time: float = None

    #def __repr__(self):
    #    return "Task id: {}, start_time: {}, end_time: {}".\
    #        format(self.id, self.start_time, self.end_time)


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
                id : int,
                env,
                flow : Flow,
                arrival_dist,
                initial_delay=0, 
                finish_time=float("inf"), 
            ):
        
        self.name = name
        self.id = id
        self.env = env
        self.arrival_dist = arrival_dist
        self.initial_delay = initial_delay
        self.finish = finish_time
        self.out = None
        self.tasks_generated = 0
        self.action = env.process(self.run())  # starts the run() method as a SimPy process

        # assign the flow
        self.flow = flow
        self.flow.entities[self.name] = self

    def run(self):
        """The generator function used in simulations.
        """
        yield self.env.timeout(self.initial_delay)
        while self.env.now < self.finish:
            # wait for next transmission
            yield self.env.timeout(self.arrival_dist())
            new_task = Task( 
                        start_time=self.env.now, 
                        id=self.tasks_generated, 
                        flow_id=self.flow.id,
                        queue_length=self.flow.entities['queue'].get_attribute('queue_length'),
            )
            self.tasks_generated += 1
            self.out.put(new_task)

    def get_attribute(self,
                    name: str,
                ):
        if name == "tasks_generated":
            return self.tasks_generated
        else:
            return None

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
                id : int, 
                env, 
                flow : Flow,
                debug=False,
            ):
        self.name = name
        self.id = id
        self.store = simpy.Store(env)
        self.env = env
        self.debug = debug
        self.action = env.process(self.run())  # starts the run() method as a SimPy process
        self.tasks_received = 0

        # assign the flow
        self.flow = flow
        self.flow.entities[self.name] = self

    def run(self):
        while True:
            task = (yield self.store.get())
            task.end_time = self.env.now

            self.tasks_received += 1
            if self.debug:
                print(task)

    def put(self, task):
        self.store.put(task)


    def get_attribute(self,
                    name: str,
                ):
        if name == "tasks_received":
            return self.tasks_received
        else:
            return None

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
                id : int,
                env, 
                flow : Flow,
                service_dist, 
                queue_limit: int=None, 
                debug: bool=False,
            ):
        self.name = name
        self.id = id
        self.store = simpy.Store(env)
        self.service_dist = service_dist
        self.env = env
        self.out = None
        self.tasks_received = 0
        self.tasks_dropped = 0
        self.queue_limit = queue_limit
        self.queue_length = 0  # Current size of the queue in bytes
        self.debug = debug
        self.is_busy = False  # Used to track if a task is currently being served
        self.action = env.process(self.run())  # starts the run() method as a SimPy process

        # assign the flow
        self.flow = flow
        self.flow.entities[self.name] = self

    def run(self):
        while True:
            task = (yield self.store.get())
            task.service_time = self.env.now
            self.busy = True
            self.queue_length -= 1
            yield self.env.timeout(self.service_dist())
            self.out.put(task)
            self.busy = False
            if self.debug:
                print(task)

    def put(self, 
            task: Task
        ):

        self.tasks_received += 1
        tmp = self.queue_length + 1

        drop = False

        if self.queue_limit is not None:       
            if tmp >= self.queue_limit:
                self.tasks_dropped += 1
                drop = True

        if not drop:
            task.queue_time = self.env.now
            self.queue_length = tmp
            return self.store.put(task)

    def get_attribute(self,
                    name: str,
                ):
        if name == "queue_length":
            return self.queue_length
        elif name == "tasks_received":
            return self.tasks_received
        elif name == "tasks_dropped":
            return self.tasks_dropped
        else:
            return None

if __name__ == "__main__":

    #arrival = functools.partial(random.expovariate, 0.8)
    arrival = functools.partial(random.uniform, 1.05, 1.05)
    service = functools.partial(random.expovariate, 1)

    env = simpy.Environment()  # Create the SimPy environment
    flow = Flow(id=0)

    # Create the start-node and end-node
    startnode = StartNode(
                        id=0,
                        name='start-node',
                        env=env, 
                        flow=flow,
                        arrival_dist=arrival)

    queue = Queue(
                id=1,
                name='queue',
                env=env,
                flow=flow,
                service_dist=service,
                queue_limit=1000)

    endnode = EndNode(
                    id=2,
                    name='end-node',
                    env=env,
                    flow=flow,
                    debug=True)

    # Wire start-node, queue, and end-node together
    startnode.out = queue
    queue.out = endnode

    # Run it
    env.run(until=8000)