import numpy as np
from pydantic import PrivateAttr

from qsimpy.random import RandomProcess


class Deterministic(RandomProcess):
    type: str = "deterministic"
    rate: int  # rho
    initial_load: int  # sigma
    duration: int  # T
    _count: int = PrivateAttr(default=0)

    def prepare_for_run(self):
        self._count = 0

    def sample(self):
        if self._count == 0:
            result = self.initial_load
        else:
            if self._count <= self.duration:
                result = self.rate
            else:
                result = 0
        self._count += 1
        return result


class Rayleigh(RandomProcess):
    type: str = "rayleigh"
    snr: np.float64
    bandwidth: np.float64
    time_slot_duration: np.float64

    def prepare_for_run(self):
        self._rng = np.random.default_rng(self.seed)

    def sample(self):
        rnd = self._rng.random()

        mean = 1.00
        exponential_rnd = -mean * np.log(1.00 - rnd)
        capacity = np.floor(
            self.time_slot_duration
            * self.bandwidth
            * np.log2(1 + exponential_rnd * (np.power(10.0, self.snr / 10.0)))
        )
        return capacity
