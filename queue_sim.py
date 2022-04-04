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

class Task(object):
    """ A very simple class that represents a task.
        This task will run through the tandem queues.
        We use a float to represent the size of the packet in bytes

        Parameters
        ----------
        start_time : float
            when the task leaves the start-node
        end_time : float
            when the task reaches the end-node
        id : int
            an identifier for the task
        flow_id : int
            small integer that can be used to identify a flow
    """
    def __init__(self, 
                start_time: float, 
                id: int, 
                flow_id: int=0,
            ):
        
        self.start_time = start_time
        self.queue_time = None
        self.service_time = None
        self.end_time = None
        self.id = id
        self.flow_id = flow_id

    def __repr__(self):
        return "Task id: {}, start_time: {}, end_time: {}".\
            format(self.id, self.start_time, self.end_time)


class StartNode(object):
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
                env, 
                arrival_dist, 
                initial_delay=0, 
                finish_time=float("inf"), 
                flow_id=0,
            ):
        
        self.env = env
        self.arrival_dist = arrival_dist
        self.initial_delay = initial_delay
        self.finish = finish_time
        self.out = None
        self.tasks_generated = 0
        self.action = env.process(self.run())  # starts the run() method as a SimPy process
        self.flow_id = flow_id

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
                        flow_id=self.flow_id,
            )
            self.tasks_generated += 1
            self.out.put(new_task)


class EndNode(object):
    """ The end-node that receives tasks and collects delay information.
        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        debug : boolean
            if true then the contents of each task will be printed as it is received.
    """
    def __init__(self, 
                env, 
                debug=False,
            ):
        self.store = simpy.Store(env)
        self.env = env
        self.debug = debug
        self.action = env.process(self.run())  # starts the run() method as a SimPy process
        self.tasks_received = 0

    def run(self):
        while True:
            task = (yield self.store.get())
            task.end_time = self.env.now

            self.tasks_received += 1
            if self.debug:
                print(task)

    def put(self, task):
        self.store.put(task)


class Queue(object):
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
                env, 
                service_dist, 
                queue_limit: int=None, 
                debug: bool=False,
            ):
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

if __name__ == "__main__":

    #arrival = functools.partial(random.expovariate, 0.8)
    arrival = functools.partial(random.uniform, 0.8, 0.8)
    service = functools.partial(random.expovariate, 1)

    env = simpy.Environment()  # Create the SimPy environment

    # Create the start-node and end-node
    startnode = StartNode(
                        env=env, 
                        arrival_dist=arrival)

    queue = Queue(
                env=env, 
                service_dist=service,
                queue_limit=10000)

    endnode = EndNode(
                    env, 
                    debug=True)

    # Wire start-node, queue, and end-node together
    startnode.out = queue
    queue.out = endnode

    # Run it
    env.run(until=8000)