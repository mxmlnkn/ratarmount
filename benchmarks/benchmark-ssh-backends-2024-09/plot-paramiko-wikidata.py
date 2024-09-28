import re
import numpy as np
import matplotlib.pyplot as plt

with open("log", 'rt') as data:
    data = [(float(line.split(' ')[1]), float(line.split(' ')[4])) for line in data]
print(data[:10])
data = np.array(data).transpose()

fig = plt.figure(figsize=(8, 6))
ax = fig.add_subplot(111, xlabel="Time / s", ylabel = "Transferred Size / GB")
ax.plot(data[1], data[0] / 1e3)
fig.tight_layout()
fig.savefig("benchmark-paramiko-wikidata.png")

plt.show()
