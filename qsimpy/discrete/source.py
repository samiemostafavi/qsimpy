from qsimpy.core import Source


class CapacitySource(Source):
    type: str = "capacitysource"

    def run(self):

        """The generator function used in simulations."""
        if self.finish_time is None:
            _finish_time = float("inf")

        while self._env.now < _finish_time:

            # get the load
            new_load = self.arrival_rp.sample()
            for _ in range(int(new_load)):

                # create the task
                new_task = self.generate_task()
                self.attributes["tasks_generated"] += 1

                # EVENT task_generation
                new_task = self.add_records(task=new_task, event_name="task_generation")

                if self._debug:
                    print(new_task)

                if self.out is not None:
                    self._out.put(new_task)

            # wait for next transmission
            yield self._env.timeout(1.00)
