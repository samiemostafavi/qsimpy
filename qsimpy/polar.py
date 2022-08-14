from __future__ import annotations

from typing import Any, Callable, Dict, FrozenSet, List

import pandas as pd
import polars as pl
import simpy
from pandas.api.types import is_bool_dtype, is_numeric_dtype, is_string_dtype
from pydantic import PrivateAttr

from .core import Entity, Model, Task


def pandas_to_polars(pddf, pp_fn) -> pl.DataFrame:

    # convert to pandas dataframe
    # if pddf was a task
    # pddf = pd.DataFrame([task])

    # apply the function
    if pp_fn is not None:
        pddf = pp_fn(pddf)

    # figure out the columns and datatypes
    # the order is very important: bool is numeric too!
    columns = []
    for col in pddf.columns:
        if is_bool_dtype(pddf[col]):
            columns.append((col, pl.Boolean))
        elif is_string_dtype(pddf[col]):
            columns.append((col, pl.Utf8))
        elif is_numeric_dtype(pddf[col]):
            columns.append((col, pl.Float64))

    return pl.DataFrame(
        pddf, columns=columns
    )  # [("col1", pl.Float32), ("col2", pl.Int64)]


class PolarSink(Entity):
    """The sink that receives all tasks and records it
    in a Spark dataframe: dropped or finished
    Spark provides the dataframe to save data larger than memory
    Parameters
    ----------
    env : simpy.Environment
        the simulation environment
    debug : boolean
        if true then the contents of each task will be printed as it is received.
    """

    type: str = "polarsink"
    events: FrozenSet[str] = {"task_reception"}
    attributes: Dict[str, Any] = {
        "tasks_received": 0,
    }
    batch_size: int = (10000,)

    _pl_received_tasks: pl.DataFrame = PrivateAttr()
    _received_tasks: List[Task] = PrivateAttr()
    _store: simpy.Store = PrivateAttr()
    _post_process_fn: Callable = PrivateAttr()

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

        self._action = model._env.process(
            self.run()
        )  # starts the run() method as a SimPy process

    def run(self):
        while True:
            task = yield self._store.get()

            # EVENT task_reception
            task = self.add_records(task=task, event_name="task_reception")

            self.attributes["tasks_received"] += 1
            if self._debug:
                print(task)

            # save the received task into the pandas dataframe
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

    @property
    def received_tasks(self):
        pddf = pd.DataFrame(self._received_tasks)
        if pddf.size != 0:  # fixed a bug
            if self._pl_received_tasks is None:
                self._pl_received_tasks = pandas_to_polars(pddf, self._post_process_fn)
            else:
                self._pl_received_tasks = self._pl_received_tasks.vstack(
                    pandas_to_polars(pddf, self._post_process_fn)
                )
        del pddf
        self._received_tasks = []
        return self._pl_received_tasks

    def put(self, task):
        self._store.put(task)
