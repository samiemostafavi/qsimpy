import seaborn as sns
import matplotlib.pyplot as plt
import tensorflow as tf
from utils import HeavyTail
import functools
import numpy as np

ht = HeavyTail(
    n = 1000000,
    gamma_concentration = 5,
    gamma_rate = 0.5,
    gpd_concentration = 0.3,
    threshold_qnt = 0.7,
    dtype = tf.float32,
)

service_delay_fn = ht.get_rnd_heavy_tail

res = []
for _ in range(100000):
    res.append(service_delay_fn())

print(np.mean(res))

fig, ax = plt.subplots()
sns.histplot(
    res,
    kde=True,
    ax = ax,
    stat="density",
).set(title="count={0}".format(len(res)))

fig.tight_layout()
plt.savefig('heavy_tail.png')









