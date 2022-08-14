from __future__ import annotations

from typing import Any, Dict, FrozenSet

import simpy
from pydantic import PrivateAttr

from qsimpy.random import RandomProcess

from .core import Entity, Model, Task


class SimpleQueue(Entity):
    """Models a FIFO queue with a service delay process and buffer size
    limit in number of tasks.
    Set the "out" member variable to the entity to receive the task.
    Parameters
    ----------
    env : simpy.Environment
        the simulation environment
    service_dist : function
        a no parameter function that returns the successive service times of the tasks
    queue_limit : integer (or None)
        a buffer size limit in number of tasks for the queue (does not include the task
         in service).
    """

    type: str = "simplequeue"
    events: FrozenSet[str] = {"task_reception", "service_start", "service_end"}
    attributes: Dict[str, Any] = {
        "tasks_received": 0,
        "tasks_dropped": 0,
        "tasks_completed": 0,
        "queue_length": 0,
        "last_service_duration": 0,
        "last_service_time": 0,
        "is_busy": False,
    }

    # Service delay random process
    service_rp: RandomProcess
    queue_limit: float = None

    _store: simpy.Store = PrivateAttr()
    _debug: bool = PrivateAttr()

    def __init__(self, **data):
        if isinstance(data["service_rp"], RandomProcess):
            data["service_rp"] = data["service_rp"].dict()
        super().__init__(**data)

    def clean_attributes(self):
        for att in self.attributes:
            if att == "is_busy":
                self.attributes[att] = False
            else:
                self.attributes[att] = 0

    def run(self) -> None:
        """
        serving tasks
        """
        while True:

            # server takes the head task from the queue
            task = yield self._store.get()
            self.attributes["queue_length"] -= 1

            # EVENT service_start
            task = self.add_records(task=task, event_name="service_start")

            # get a service duration
            self.attributes["is_busy"] = True
            new_service_duration = self.service_rp.sample()
            self.attributes["last_service_duration"] = new_service_duration
            self.attributes["last_service_time"] = self._env.now

            # wait until the task is served
            yield self._env.timeout(new_service_duration)
            self.attributes["is_busy"] = False

            # EVENT service_end
            task = self.add_records(task=task, event_name="service_end")
            self.attributes["tasks_completed"] += 1

            if self._debug:
                print(task)

            # put it on the output
            if self.out is not None:
                self._out.put(task)

    def prepare_for_run(self, model: Model, env: simpy.Environment, debug: bool):
        self._model = model
        self._env = env
        self._debug = debug

        if self.out is not None:
            self._out = model.entities[self.out]
        if self.drop is not None:
            self._drop = model.entities[self.drop]

        self.service_rp.prepare_for_run()
        self._store = simpy.Store(env)
        self._action = model._env.process(
            self.run()
        )  # starts the run() method as a SimPy process

    def put(self, task: Task) -> None:
        """
        queuing tasks
        """

        # increase the received counter
        self.attributes["tasks_received"] += 1

        # EVENT task_reception
        task = self.add_records(task=task, event_name="task_reception")

        # check if we need to drop the task due to buffer size limit
        drop = False
        if self.queue_limit is not None:
            if self.attributes["queue_length"] + 1 >= self.queue_limit:
                drop = True

        if drop:
            # drop the task
            self.attributes["tasks_dropped"] += 1
            if self.drop is not None:
                self._drop.put(task)
        else:
            # store the task in the queue
            self.attributes["queue_length"] += 1
            self._store.put(task)
