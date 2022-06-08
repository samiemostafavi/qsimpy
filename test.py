import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import matplotlib.pyplot as plt
import seaborn as sns


file_addresses = ['dataset.parquet']

table = pa.concat_tables(
    pq.read_table(
        file_address,columns=None,
    ) for file_address in file_addresses
)

df = table.to_pandas()

print(df)

# find 10 most common queue_length occurances
n = 4; n1 = 2; n2 = 2
values_count = df[['queue_length1','queue_length2','queue_length3']].value_counts()[:n].index.tolist()
print("{0} most common queue states: {1}".format(n,values_count))
#values_count = [(1,1,1),(0,0,0),(0,5,0),(0,10,0)]

# plot the conditional distributions of them
fig, axes = plt.subplots(ncols=n1, nrows=n2)

for i, ax in zip(range(n), axes.flat):
    conditional_df = df[
        (df.queue_length1==values_count[i][0]) & 
        (df.queue_length2==values_count[i][1]) & 
        (df.queue_length3==values_count[i][2])
    ]
    sns.histplot(
        conditional_df['end2end_delay'],
        kde=True, 
        ax=ax,
        stat="density",
    ).set(title="x={0}, count={1}".format(values_count[i],len(conditional_df)))
    ax.title.set_size(10)

fig.tight_layout()
plt.savefig('conditional_delay.png')