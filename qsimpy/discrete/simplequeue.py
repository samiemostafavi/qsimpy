from qsimpy.simplequeue import SimpleQueue


class CapacityQueue(SimpleQueue):
    type: str = "capacityqueue"

    def run(self) -> None:
        """
        serving tasks
        """
        while True:

            # start of the timeslot
            # get the capacity
            new_capacity = self.service_rp.sample()

            # start serving tasks
            backlog = len(self._store.items)
            num_service = min(backlog, new_capacity)

            for _ in range(int(num_service)):
                task = yield self._store.get()
                self.attributes["queue_length"] -= 1

                # EVENT service_time
                task = self.add_records(task=task, event_name="service_time")
                self.attributes["tasks_completed"] += 1

                if self._debug:
                    print(task)

                # put it on the output
                if self.out is not None:
                    self._out.put(task)

            # server runs at every timeslot
            yield self._env.timeout(1.00)
