import numpy as np
from scipy.stats import gamma

rng = np.random.default_rng(1234)

np_shape = 5.00
np_scale = 0.5
n = 10000
ldp = 0.1

samples = rng.gamma(shape=np_shape, scale=np_scale, size=n)
quant = np.quantile(samples, 1.00 - ldp)

print(quant)

# sample conditioned on a longer_delay_prob
time_in_service = gamma.ppf(
    q=1.00 - ldp,
    a=np_shape,
    loc=0,
    scale=np_scale,
)
print(time_in_service)
