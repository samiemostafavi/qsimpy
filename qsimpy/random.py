from typing import Tuple

import numpy as np
from pydantic import BaseModel, PrivateAttr
from scipy.stats import gamma


class RandomProcess(BaseModel):
    dtype: str
    type: str
    seed: int

    _rng: np.random.Generator = PrivateAttr()
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
            raise TypeError(f"Unsupported sub-type: {data_type}")

        return sub(**data)

    def prepare_for_run(self):
        pass

    def get(self, inp):
        if inp == "type":
            return self.type

    @classmethod
    def parse_obj(cls, obj):
        return cls._convert_to_real_type_(obj)

    def sample(self):
        pass

    def sample_n(
        self,
        n: int,
    ):
        pass

    def prob(
        self,
        y,
    ):
        pass

    def cdf(
        self,
        y,
    ):
        pass


class Deterministic(RandomProcess):
    type: str = "deterministic"
    rate: np.float64

    def sample_n(
        self,
        n: int,
    ):
        return self._rng.uniform(1.00 / self.rate, 1.00 / self.rate, size=n)

    def sample(self):
        return self._rng.uniform(1.00 / self.rate, 1.00 / self.rate, size=1)[0]

    def prepare_for_run(self):
        self._rng = np.random.default_rng(self.seed)


class Exponential(RandomProcess):
    type: str = "exponential"
    rate: np.float64

    def sample_n(
        self,
        n: int,
    ):
        return self._rng.exponential(scale=1.0 / self.rate, size=n)

    def sample(self):
        return self._rng.exponential(scale=1.0 / self.rate, size=1)[0]

    def prepare_for_run(self):
        self._rng = np.random.default_rng(self.seed)


class Gamma(RandomProcess):
    type: str = "gamma"
    shape: np.float64  # k or shape in numpy, a or shape in scipy
    scale: np.float64  # theta or scale in numpy, beta or 1/scale in scipy

    _rng_state: Tuple

    def sample_n(
        self,
        n: int,
    ):
        return self._rng.gamma(shape=self.shape, scale=self.scale, size=n)

    def sample(self):
        return self._rng.gamma(shape=self.shape, scale=self.scale, size=1)[0]

    def sample_ldp(self, ldp: float):
        # sample conditioned on a longer_delay_prob
        # NOTE: tests show scale=self.scale is correct
        # but the document says 1.00 / self.scale
        time_in_service = gamma.ppf(
            q=1.00 - ldp,
            a=self.shape,
            loc=0,
            scale=self.scale,
        )
        # if ldp==1.00
        if time_in_service == 0:
            return self.sample()

        sample = 0
        while sample < time_in_service:
            sample = self.sample()

        return sample - time_in_service

    def prepare_for_run(self):
        self._rng = np.random.default_rng(self.seed)

    def cdf(self, y):
        # NOTE: tests show scale=self.scale is correct
        # but the document says 1.00 / self.scale
        return gamma.cdf(y, self.shape, loc=0, scale=self.scale)
