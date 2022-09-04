from __future__ import annotations

from dataclasses import field, make_dataclass
from typing import Any, Dict, FrozenSet

import pandas as pd
import simpy
from pydantic import PrivateAttr

from .core import Entity, Model, Task
from .polar import PolarSink, pandas_to_polars
from .utils import get_all_values


class GymSource(Entity):
    """Generates gym tasks
    Set the "out" member variable to the entity to receive the task.

    Parameters
    ----------
    env : Environment
        the QSimPy simulation environment
    main_task_type : str
        type of the tasks being generated
    traffic_task_type : str
        type of the tasks being generated
    traffic_task_num : int
        the number of traffic tasks to go before the main task
    initial_delay : number
        Starts task generation after an initial delay. Default = 0
    finish_time : number
        Stops generation at the finish time. Default is infinite
    """

    type: str = "gymsource"
    events: FrozenSet[str] = {"task_generation"}
    attributes: Dict[str, Any] = {
        "main_tasks_generated": 0,
        "traffic_tasks_generated": 0,
    }
    main_task_num: int = 1
    main_task_type: str
    traffic_task_type: str
    traffic_task_num: int
    initial_delay: float = 0
    finish_time: float = None

    _store: simpy.Store = PrivateAttr()

    def prepare_for_run(self, model: Model, env: simpy.Environment, debug: bool):
        self._model = model
        self._env = env
        self._debug = debug

        if self.out is not None:
            self._out = model.entities[self.out]
        if self.drop is not None:
            self._drop = model.entities[self.drop]

        self._store = simpy.Store(env)
        # starts the run() method as a SimPy process
        self._action = model._env.process(self.run())

    def clean_attributes(self):
        for att in self.attributes:
            self.attributes[att] = 0

    def generate_main_tasks(self, n: int):
        new_tasks = []
        for i in range(n):
            new_task = Task(
                id=self.attributes["main_tasks_generated"],
                task_type=self.main_task_type,
            )
            # create a GeneratedTask dataclass with the fields that come from the
            # timestamps and attributes form the fields for make_dataclass
            if self._model.task_records:
                fields = [
                    (name, float, field(default=-1))
                    for name in get_all_values(self._model.task_records)
                ]
                fields.append(("is_last_main", bool, field(default=False)))
                # call make_dataclass
                new_task.__class__ = make_dataclass(
                    "GeneratedTask", fields=fields, bases=(Task,)
                )
            # if it is the last main task
            if i == n - 1:
                new_task.is_last_main = True
            self.attributes["main_tasks_generated"] += 1
            # EVENT task_generation
            new_task = self.add_records(task=new_task, event_name="task_generation")
            new_tasks.append(new_task)

        return new_tasks

    def generate_traffic_tasks(self, n: int):

        traffic_tasks = [
            Task(
                id=self.attributes["traffic_tasks_generated"],
                task_type=self.traffic_task_type,
            )
            for _ in range(n)
        ]

        self.attributes["traffic_tasks_generated"] += n

        return traffic_tasks

    def run(self):
        """The generator function used in simulations."""
        yield self._env.timeout(self.initial_delay)
        if self.finish_time is None:
            _finish_time = float("inf")
        while self._env.now < _finish_time:
            # generate and send the traffic
            traffic_tasks = self.generate_traffic_tasks(self.traffic_task_num)
            if self.out is not None:
                for traffic_task in traffic_tasks:
                    self._out.put(traffic_task)

            # generate and send the main task
            main_tasks = self.generate_main_tasks(self.main_task_num)
            if self.out is not None:
                for main_task in main_tasks:
                    self._out.put(main_task)

            # wait for the next transmission
            yield self._store.get()

    def put(self, start_msg):
        self._store.put(start_msg)


class GymSink(PolarSink):
    """
    A sink entity that drops traffic tasks
    """

    type: str = "gymsink"

    def put(self, task: Task):
        if task.task_type == "main":
            self._store.put(task)
        else:
            del task

    def run(self):
        while True:
            task = yield self._store.get()

            # EVENT task_reception
            task = self.add_records(task=task, event_name="task_reception")

            self.attributes["tasks_received"] += 1
            if self._debug:
                print(task)

            self._received_tasks.append(task)

            if len(self._received_tasks) >= self.batch_size:
                pddf = pd.DataFrame(self._received_tasks)
                # save the received task into a Polars dataframe
                if self._pl_received_tasks is None:
                    self._pl_received_tasks = pandas_to_polars(
                        pddf, self._post_process_fn
                    )
                else:
                    self._pl_received_tasks = self._pl_received_tasks.vstack(
                        pandas_to_polars(pddf, self._post_process_fn)
                    )
                del task, pddf
                self._received_tasks = []

            if (self.out is not None) and (task.is_last_main):
                # send the start message to the source
                self._out.put(Task(id=0, task_type="start_msg"))
