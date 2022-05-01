#!/usr/bin/env python3
#
# A script to filter out a small portion of the test data, for documentation.
# (We only keep data for Feb 2020, in California and Hawaii.)
#
# Run after gen_temperature_dat.py.

import datetime
import re

import pandas as pd
import numpy as np

A = np.load('/home/yongjik/PLT/private/test1.npz')

names = A['names']
X = A['timestamps']
Y = A['temperatures']
start_idxs = A['start_idxs']

T0 = datetime.datetime(2020, 2, 1, tzinfo=datetime.timezone.utc)
T0 = T0.timestamp()
T1 = datetime.datetime(2020, 3, 1, tzinfo=datetime.timezone.utc)
T1 = T1.timestamp()

out = []
for idx, name in enumerate(names):
    if not re.search(r'(CA|HI) US$', name): continue

    start_idx = start_idxs[idx]
    end_idx = start_idxs[idx + 1] if idx < len(start_idxs) - 1 else len(X)

    X1 = X[start_idx:end_idx]
    Y1 = Y[start_idx:end_idx]
    filtered = (T0 <= X1) & (X1 < T1)
    X1 = X1[filtered]
    Y1 = Y1[filtered]

    for x, y in zip(X1, Y1):
        out.append((name, x, y))

df = pd.DataFrame(out, columns=['name', 'timestamp', 'temperature'])
df.to_csv('CA_HI_Feb2020.csv.gz', index=False)
