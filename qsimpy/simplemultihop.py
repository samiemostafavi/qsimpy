from __future__ import annotations

from typing import Any, Dict, List, Set

import simpy
from pydantic import PrivateAttr

from qsimpy.random import RandomProcess

from .core import Entity, Model, Task


class SimpleMultiHop(Entity):
    """Models a multihop FIFO queue with a service delay process and buffer size
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

    type: str = "simplemultihop"
    n_hops: int = 1
    events: Set[str] = set()
    attributes: Dict[str, Any] = {}

    # Service delay random processes
    service_rp: List[RandomProcess]
    queue_limit: List = None

    _store: List[simpy.Store] = PrivateAttr()
    _debug: bool = PrivateAttr()

    def __init__(self, **data):
        for hop in range(data["n_hops"]):
            if isinstance(data["service_rp"][hop], RandomProcess):
                data["service_rp"][hop] = data["service_rp"][hop].dict()

        data["events"] = set()
        for hop in range(data["n_hops"]):
            data["events"].update(
                [
                    f"task_reception_h{hop}",
                    f"service_start_h{hop}",
                    f"service_end_h{hop}",
                ]
            )

        data["attributes"] = {}
        for hop in range(data["n_hops"]):
            data["attributes"].update(
                {
                    f"tasks_received_h{hop}": 0,
                    f"tasks_dropped_h{hop}": 0,
                    f"tasks_completed_h{hop}": 0,
                    f"queue_length_h{hop}": 0,
                    f"last_service_duration_h{hop}": 0,
                    f"last_service_time_h{hop}": 0,
                    f"is_busy_h{hop}": False,
                }
            )

        if "queue_limit" not in data.keys():
            data["queue_limit"] = [None] * data["n_hops"]

        super().__init__(**data)

    def clean_attributes(self):
        for att in self.attributes:
            if "is_busy" in att:
                self.attributes[att] = False
            else:
                self.attributes[att] = 0

    def prepare_for_run(self, model: Model, env: simpy.Environment, debug: bool):
        self._model = model
        self._env = env
        self._debug = debug

        if self.out is not None:
            self._out = model.entities[self.out]
        if self.drop is not None:
            self._drop = model.entities[self.drop]

        for service in self.service_rp:
            service.prepare_for_run()

        self._store = []
        self._action: List = []
        for hop in range(self.n_hops):
            self._store.append(simpy.Store(env))
            self._action.append(model._env.process(self.run(hop)))

    def run(self, hop: int) -> None:
        """
        serving tasks
        """
        while True:

            # server takes the head task from the queue
            task = yield self._store[hop].get()
            self.attributes[f"queue_length_h{hop}"] -= 1

            # EVENT service_start
            task = self.add_records(task=task, event_name=f"service_start_h{hop}")

            # get a service duration
            self.attributes[f"is_busy_h{hop}"] = True
            # conditional sampling for gym
            if hasattr(task, "longer_delay_prob"):
                new_service_duration = self.service_rp[hop].sample_ldp(
                    task.longer_delay_prob,
                )
            else:
                new_service_duration = self.service_rp[hop].sample()
            self.attributes[f"last_service_duration_h{hop}"] = new_service_duration
            self.attributes[f"last_service_time_h{hop}"] = self._env.now

            # wait until the task is served
            yield self._env.timeout(new_service_duration)
            self.attributes[f"is_busy_h{hop}"] = False

            # EVENT service_end
            task = self.add_records(task=task, event_name=f"service_end_h{hop}")
            self.attributes[f"tasks_completed_h{hop}"] += 1

            if self._debug:
                print(f"hop: {hop}, {task}")

            # put it on the output or the next hop
            if hop == self.n_hops - 1:
                if self.out is not None:
                    self._out.put(task)
            else:
                self.put_midhop(task, hop + 1)

    def put_midhop(self, task: Task, hop: int) -> None:
        """
        queuing tasks
        """

        # increase the received counter
        self.attributes[f"tasks_received_h{hop}"] += 1

        # EVENT task_reception
        task = self.add_records(task=task, event_name=f"task_reception_h{hop}")

        # check if we need to drop the task due to buffer size limit
        drop = False
        if self.queue_limit[hop] is not None:
            if self.attributes[f"queue_length_h{hop}"] + 1 >= self.queue_limit[hop]:
                drop = True

        if drop:
            # drop the task
            self.attributes[f"tasks_dropped_h{hop}"] += 1
            if self.drop is not None:
                self._drop.put(task)
        else:
            # store the task in the queue
            self.attributes[f"queue_length_h{hop}"] += 1
            self._store[hop].put(task)

    def put(self, task: Task) -> None:
        """
        queuing tasks
        """

        self.put_midhop(task=task, hop=0)
