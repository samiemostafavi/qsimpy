from __future__ import annotations

import simpy
from dataclasses import dataclass,make_dataclass,field
from typing import Dict, FrozenSet, Any, Callable


@dataclass(frozen=False, eq=True)
class Task():
    """ A very simple dataclass that represents a task.
        This task will run through the tandem queues.
        We use a float to represent the size of the packet in bytes

        Parameters
        ----------
        id : int
            an identifier for the task
        env_name : str
            string that can be used to identify a qsimpy env
    """
    id: int
    env_name: str


def get_all_values(d):
    """
    get_all_values returns all the leaves of a dictionary

    :param d: input dictionary
    :return: list of values
    """ 
    if isinstance(d, dict):
        for v in d.values():
            yield from get_all_values(v)
    elif isinstance(d, list):
        for v in d:
            yield from get_all_values(v)
    else:
        yield d 


class Environment(simpy.Environment):
    """ A class for keeping the entities, simpy.Environment, and accessing 
        the entities' attributes
    """
    name : str
    entities : Dict[str,Entity]
    def __init__(self, name):
        self.name = name
        self.entities = {}

        # initialize the simpy.Environment()
        super().__init__()
    
    def add_entity(self, entity : Entity) -> None:
        self.entities[entity.name] = entity


class Entity():
    name : str
    env : Environment
    attributes : Dict[str,Any]
    events : FrozenSet[str]
    def __init__(self,
                name : str,
                env : Environment,
                attributes : Dict[str,Any],
                events : FrozenSet[str],
                records_config: Dict,
    ):
        self.name = name
        self.env = env
        self.attributes = attributes
        self.events = events
        self.records_config = records_config

        # assign the run function
        self.action = self.env.process(self.run())  # starts the run() method as a SimPy process

        # add it to the environment
        self.env.add_entity(self)

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

        # record the requested timestamp
        ts_dict = self.records_config.get('timestamps',{}).get(self.name,{})
        if event_name in self.records_config.get('timestamps',{}).get(self.name,{}):
            task.__setattr__(ts_dict[event_name],self.env.now)

        # record the requested attributes
        att_dict = self.records_config.get('attributes',{}).get(self.name,{}).get(event_name,{})
        for entity_name in att_dict:
            for attribute in att_dict[entity_name]:
                value = self.env.entities[entity_name].get_attribute(attribute)
                task.__setattr__(att_dict[entity_name][attribute],value)

        return task


class StartNode(Entity):
    """ Generates tasks with given inter-arrival time distribution.
        Set the "out" member variable to the entity to receive the task.

        Parameters
        ----------
        env : Environment
            the QSimPy simulation environment
        arrival_dist : function
            a no parameter function that returns the successive inter-arrival times of the tasks
        initial_delay : number
            Starts task generation after an initial delay. Default = 0
        finish_time : number
            Stops generation at the finish time. Default is infinite
    """
    def __init__(self,
                name : str,
                env : Environment,
                records_config: Dict,
                arrival_dist : Callable,
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
        super().__init__(name,env,attributes,events,records_config)

    def generate_task(self):
        new_task = Task(
            id=self.attributes['tasks_generated'], 
            env_name=self.env.name
        )

        # create a GeneratedTask dataclass with the fields that come from the timestamps and attributes
        # form the fields for make_dataclass
        fields = [ (name, float, field(default=-1)) for name in get_all_values(self.records_config) ]
        # call make_dataclass
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
                env : Environment, 
                records_config: Dict,
                debug=False,
            ):

        self.store = simpy.Store(env)
        self.debug = debug
        self.received_tasks = []

        # initialize the attributes
        attributes = {
            'tasks_received':0,
        }

        # advertize the events
        events = { 'task_reception' }

        # initialize the entity
        super().__init__(name,env,attributes,events,records_config)

    def run(self):
        while True:
            task = (yield self.store.get())

            # add event records
            task = self.add_records(task=task, event_name='task_reception')

            self.attributes['tasks_received'] += 1
            if self.debug:
                print(task)

            self.received_tasks.append(task)
            

    def put(self, task):
        self.store.put(task)