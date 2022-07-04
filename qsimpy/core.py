from __future__ import annotations
from html import entities

import simpy
from dataclasses import dataclass,make_dataclass,field
from typing import Dict, FrozenSet, Any, Callable, List
import polars as pl
import pandas as pd
import numpy as np
from pydantic import BaseModel, PrivateAttr, Field

from .utils import get_all_values
from .random import RandomProcess


@dataclass(frozen=False, eq=True)
class Task():
    """ A very simple dataclass that represents a task.
        This task will run through the tandem queues.
        We use a float to represent the size of the packet in bytes

        Parameters
        ----------
        id : int
            an identifier for the task
        task_type : str
            string that can be used to identify a task type e.g. cross traffic
    """
    id: int
    task_type: str


class Model(BaseModel):
    """ A class for keeping the entities, simpy.Environment, and accessing 
        the entities' attributes
    """
    name : str
    task_records : Dict = {}
    entities : Dict[str,Entity] = {}

    # Pydantic will exclude the class variables which begin with an underscore
    _env : simpy.Environment = PrivateAttr()

    def __init__(self,**data):
        super().__init__(**data)
    
    def add_entity(self, entity : Entity) -> None:
        self.entities[entity.name] = entity

    def set_task_records(self, task_records : Dict) -> None:
        self.task_records = task_records

    def prepare_for_run(self, debug: bool, clean: bool = False) -> None:
        self._env = simpy.Environment()
        for entity in self.entities.values():
            entity.prepare_for_run(self,self._env,debug)
            if clean:
                entity.clean_attributes()

    @property
    def env(self):
        return self._env

class Entity(BaseModel):
    name : str
    type : str
    events : FrozenSet[str]
    attributes : Dict[str,Any]
    out : str = None
    drop : str = None

    _model : Model = PrivateAttr()
    _env : simpy.Environment = PrivateAttr()
    _debug : bool = PrivateAttr()
    _action : Any = PrivateAttr()
    _out : Entity = PrivateAttr()
    _drop : Entity = PrivateAttr()
    _subtypes_ = dict()

    def __init_subclass__(cls, type=None):
        cls._subtypes_[type or cls.__name__.lower()] = cls

    @classmethod
    def __get_validators__(cls):
        yield cls._convert_to_real_type_

    @classmethod
    def _convert_to_real_type_(cls, data):
        data_type = data.get("type")

        if data_type is None:
            raise ValueError("Missing 'type'")

        sub = cls._subtypes_.get(data_type)

        if sub is None:
            raise TypeError(f"Unsupport sub-type: {data_type}")

        return sub(**data)

    @classmethod
    def parse_obj(cls, obj):
        return cls._convert_to_real_type_(obj)

    def get(self, inp):
        if inp == "type":
            return self.type

    def clean_attributes(self):
        pass

    def prepare_for_run(self, model: Model, env: simpy.Environment, debug: bool):
        self._model = model
        self._env = env
        self._debug = debug

        if self.out is not None:
            self._out = model.entities[self.out]
        if self.drop is not None:
            self._drop = model.entities[self.drop]

        self._action = model._env.process(self.run())  # starts the run() method as a SimPy process

    def run(self) -> None:
        pass
    def put(self, Task) -> None:
        pass

    def get_attribute(self,
                    name: str,
                ) -> Any:
        return self.attributes[name]
    def get_all_attributes(self) -> Dict[str,Any]:
        return self.attributes
    def get_events_names(self) -> FrozenSet[str]:
        return self.events
    def add_records(self,
                    task: Task,
                    event_name: str,
                ) -> Task:

        if self._model.task_records:
            # record the requested timestamp
            ts_dict = self._model.task_records.get('timestamps',{}).get(self.name,{})
            if event_name in self._model.task_records.get('timestamps',{}).get(self.name,{}):
                task.__setattr__(ts_dict[event_name],self._env.now)

            # record the requested attributes
            att_dict = self._model.task_records.get('attributes',{}).get(self.name,{}).get(event_name,{})
            for entity_name in att_dict:
                for attribute in att_dict[entity_name]:
                    value = self._model.entities[entity_name].get_attribute(attribute)
                    task.__setattr__(att_dict[entity_name][attribute],value)

        return task

Model.update_forward_refs()
Entity.update_forward_refs()

class Source(Entity):
    """ Generates tasks with given inter-arrival time distribution.
        Set the "out" member variable to the entity to receive the task.

        Parameters
        ----------
        env : Environment
            the QSimPy simulation environment
        task_type : str
            type of the tasks being generated
        arrival_rp : RandomProcess
            A RandomProcess object that its sample_n fn returns the successive inter-arrival times of the tasks
        initial_delay : number
            Starts task generation after an initial delay. Default = 0
        finish_time : number
            Stops generation at the finish time. Default is infinite
    """
    type : str = 'source'
    events : FrozenSet[str] = { 
        'task_generation' 
    }
    attributes : Dict[str,Any] = {
        'tasks_generated' : 0,
    }
    task_type : str
    initial_delay : float = 0
    finish_time : float = float("inf")

    # Arrival random process
    arrival_rp : RandomProcess

    def __init__(self, **data):
        if isinstance(data['arrival_rp'], RandomProcess):
            data['arrival_rp'] = data['arrival_rp'].dict()
        super().__init__(**data)

    def prepare_for_run(self, model: Model, env: simpy.Environment, debug: bool):
        self._model = model
        self._env = env
        self._debug = debug

        if self.out is not None:
            self._out = model.entities[self.out]
        if self.drop is not None:
            self._drop = model.entities[self.drop]

        self.arrival_rp.prepare_for_run()
        self._action = model._env.process(self.run())  # starts the run() method as a SimPy process

    def clean_attributes(self):
        for att in self.attributes:
            self.attributes[att] = 0

    def generate_task(self):
        new_task = Task(
            id=self.attributes['tasks_generated'], 
            task_type=self.task_type,
        )

        # create a GeneratedTask dataclass with the fields that come from the timestamps and attributes
        # form the fields for make_dataclass
        if self._model.task_records:
            fields = [ (name, float, field(default=-1)) for name in get_all_values(self._model.task_records) ]
            # call make_dataclass
            new_task.__class__ = make_dataclass('GeneratedTask', fields=fields, bases=(Task,))
            return new_task
        else:
            return new_task

    def run(self):
        """The generator function used in simulations.
        """
        yield self._env.timeout(self.initial_delay)
        while self._env.now < self.finish_time:
            # wait for next transmission
            yield self._env.timeout(self.arrival_rp.sample())
            new_task = self.generate_task()
            self.attributes['tasks_generated'] += 1
            
            # EVENT task_generation
            new_task = self.add_records(task=new_task, event_name='task_generation')

            if self._debug:
                print(new_task)

            if self.out is not None:
                self._out.put(new_task)


class Sink(Entity):
    """ The sink that receives all tasks: dropped or finished
        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        debug : boolean
            if true then the contents of each task will be printed as it is received.
    """
    type : str = 'sink'
    events : FrozenSet[str] = { 
        'task_reception'
    }
    attributes : Dict[str,Any] = {
        'tasks_received':0,
    }

    _received_tasks : List[Task] = PrivateAttr()
    _store : simpy.Store = PrivateAttr()

    def clean_attributes(self):
        for att in self.attributes:
            self.attributes[att] = 0

    def prepare_for_run(self, model: Model, env: simpy.Environment, debug: bool):
        self._model = model
        self._env = env
        self._debug = debug

        if self.out is not None:
            self._out = model.entities[self.out]
        if self.drop is not None:
            self._drop = model.entities[self.drop]

        self._store = simpy.Store(env)
        self._received_tasks = []

        self._action = model._env.process(self.run())  # starts the run() method as a SimPy process

    def run(self):
        while True:
            task = (yield self._store.get())

            # EVENT task_reception
            task = self.add_records(task=task, event_name='task_reception')

            self.attributes['tasks_received'] += 1
            if self._debug:
                print(task)

            self._received_tasks.append(task)
            
    def put(self, task):
        self._store.put(task)

    @property
    def received_tasks(self):
        return self._received_tasks


class Node(Entity):
    """ The node object that receives tasks and collects delay information and sends it
        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        debug : boolean
            if true then the contents of each task will be printed as it is received.
    """

    type : str = 'node'
    events : FrozenSet[str] = { 
        'task_reception'
    }
    attributes : Dict[str,Any] = {
        'tasks_received':0,
    }

    _store : simpy.Store = PrivateAttr()

    def clean_attributes(self):
        for att in self.attributes:
            self.attributes[att] = 0

    def prepare_for_run(self, model: Model, env: simpy.Environment, debug: bool):
        self._model = model
        self._env = env
        self._debug = debug

        if self.out is not None:
            self._out = model.entities[self.out]
        if self.drop is not None:
            self._drop = model.entities[self.drop]

        self._store = simpy.Store(env)

        self._action = model._env.process(self.run())  # starts the run() method as a SimPy process

    def run(self):
        while True:
            task = (yield self._store.get())

            # EVENT task_reception
            task = self.add_records(task=task, event_name='task_reception')

            self.attributes['tasks_received'] += 1
            if self._debug:
                print(task)

            if self.out is not None:
                self._out.put(task)
            
    def put(self, task):
        self._store.put(task)
        

from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_bool_dtype

def pandas_to_polars(pddf, pp_fn) -> pl.DataFrame:

    # convert to pandas dataframe
    # if pddf was a task
    #pddf = pd.DataFrame([task])

    #apply the function
    if pp_fn is not None:
        pddf = pp_fn(pddf)

    # figure out the columns and datatypes
    # the order is very important: bool is numeric too!
    columns = []
    for col in pddf.columns:
        if is_bool_dtype(pddf[col]):
            columns.append((col,pl.Boolean))
        elif is_string_dtype(pddf[col]):
            columns.append((col,pl.Utf8))
        elif is_numeric_dtype(pddf[col]):
            columns.append((col,pl.Float64))
        

    return pl.DataFrame(pddf, columns=columns) # [("col1", pl.Float32), ("col2", pl.Int64)]


class PolarSink(Entity):
    """ The sink that receives all tasks and records it in a Spark dataframe: dropped or finished
        Spark provides the dataframe to save data larger than memory
        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        debug : boolean
            if true then the contents of each task will be printed as it is received.
    """

    type : str = 'polarsink'
    events : FrozenSet[str] = { 
        'task_reception'
    }
    attributes : Dict[str,Any] = {
        'tasks_received':0,
    }
    batch_size : int = 10000,
    post_process_fn_str : str = None

    _pl_received_tasks : pl.DataFrame = PrivateAttr()
    _received_tasks : List[Task] = PrivateAttr()
    _store : simpy.Store = PrivateAttr()
    _post_process_fn : Callable = PrivateAttr()

    def clean_attributes(self):
        for att in self.attributes:
            self.attributes[att] = 0

    def prepare_for_run(self, model: Model, env: simpy.Environment, debug: bool):
        self._model = model
        self._env = env
        self._debug = debug

        if self.out is not None:
            self._out = model.entities[self.out]
        if self.drop is not None:
            self._drop = model.entities[self.drop]

        self._store = simpy.Store(env)
        self._received_tasks = []
        self._pl_received_tasks = None

        # post process function
        if self.post_process_fn_str is not None:
            exec(self.post_process_fn_str, None, globals() )
            self._post_process_fn = user_fn

        self._action = model._env.process(self.run())  # starts the run() method as a SimPy process

    def set_post_process_fn(self, fn_str : str):
        self.post_process_fn_str = fn_str

    def run(self):
        while True:
            task = (yield self._store.get())

            # EVENT task_reception
            task = self.add_records(task=task, event_name='task_reception')

            self.attributes['tasks_received'] += 1
            if self._debug:
                print(task)

            # save the received task into the pandas dataframe
            self._received_tasks.append(task)

            if len(self._received_tasks) >= self.batch_size:
                pddf = pd.DataFrame(self._received_tasks)
                # save the received task into a Polars dataframe
                if self._pl_received_tasks is None:
                    self._pl_received_tasks = pandas_to_polars(pddf,self._post_process_fn)
                else:
                    self._pl_received_tasks = self._pl_received_tasks.vstack(
                        pandas_to_polars(pddf,self._post_process_fn)
                    )
                del task, pddf
                self._received_tasks = []

    @property
    def received_tasks(self):
        pddf = pd.DataFrame(self._received_tasks)
        if self._pl_received_tasks is None:
            self._pl_received_tasks = pandas_to_polars(pddf,self._post_process_fn)
        else:
            self._pl_received_tasks = self._pl_received_tasks.vstack(
                pandas_to_polars(pddf,self._post_process_fn)
            )
        del pddf
        self._received_tasks = []
        return self._pl_received_tasks

    def put(self, task):
        self._store.put(task)