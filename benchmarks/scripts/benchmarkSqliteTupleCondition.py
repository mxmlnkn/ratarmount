#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
import time

import numpy as np


schema = """
CREATE TABLE "files" (
    "path" VARCHAR(65535),
    "name" VARCHAR(65535),
    "mode" INTEGER
);
"""

queryUsingTuples = """
SELECT path,name
FROM "files"
WHERE (path,name) NOT IN (
    SELECT path,name FROM "files" WHERE mode == 1
)
ORDER BY "path","name";
"""

queryUsingConcatenation = """
SELECT path,name
FROM "files"
WHERE path || "/" || name NOT IN (
    SELECT path || "/" || name FROM "files" WHERE mode == 1
)
ORDER BY "path","name";
"""


def createTable(nRows, fileNameLength, modeProbability):
    db = sqlite3.connect(":memory:")
    db.executescript(schema)

    paths = [os.urandom(fileNameLength // 2).hex() for j in range(nRows)]
    names = [os.urandom(fileNameLength // 2).hex() for j in range(nRows)]
    modes = list(np.random.rand(nRows) <= modeProbability)
    db.executemany('INSERT INTO files VALUES (?,?,?)', zip(paths, names, modes))

    db.commit()
    return db


def benchmarkQuery(nRows, fileNameLength, modeProbability, query):
    db = createTable(nRows, fileNameLength, modeProbability)

    t0 = time.time()
    db.execute(query).fetchall()
    t1 = time.time()

    return t1 - t0


if __name__ == '__main__':
    for modeProbability in np.arange(6) * 0.2:
        print("Using mode probability:", modeProbability)
        print(" Using Tuples:")
        for nRows in 10 ** np.arange(3, 7):
            print(" ", nRows, benchmarkQuery(nRows, 128, 0.5, queryUsingTuples))

        print()
        print(" Using Concatenation:")
        for nRows in 10 ** np.arange(3, 7):
            print(" ", nRows, benchmarkQuery(nRows, 128, 0.5, queryUsingConcatenation))
        print()


"""
Results:

Using mode probability: 0.0
 Using Tuples:
  1000 0.0009732246398925781
  10000 0.012987375259399414
  100000 0.14557600021362305
  1000000 1.5429859161376953

 Using Concatenation:
  1000 0.0009105205535888672
  10000 0.012718915939331055
  100000 0.15746307373046875
  1000000 1.7333405017852783


Using mode probability: 0.2
 Using Tuples:
  1000 0.0007994174957275391
  10000 0.011710882186889648
  100000 0.1446692943572998
  1000000 1.564864158630371

 Using Concatenation:
  1000 0.0009160041809082031
  10000 0.013094663619995117
  100000 0.1578364372253418
  1000000 1.731074571609497


Using mode probability: 0.4
 Using Tuples:
  1000 0.0008268356323242188
  10000 0.012100458145141602
  100000 0.14467072486877441
  1000000 1.526106834411621

 Using Concatenation:
  1000 0.0009052753448486328
  10000 0.012700796127319336
  100000 0.15579938888549805
  1000000 1.6879661083221436


Using mode probability: 0.6000000000000001
 Using Tuples:
  1000 0.0008080005645751953
  10000 0.011435985565185547
  100000 0.14171934127807617
  1000000 1.5599656105041504

 Using Concatenation:
  1000 0.0009360313415527344
  10000 0.01266336441040039
  100000 0.1569380760192871
  1000000 1.7085907459259033


Using mode probability: 0.8
 Using Tuples:
  1000 0.0008215904235839844
  10000 0.01145172119140625
  100000 0.14081144332885742
  1000000 1.5523502826690674

 Using Concatenation:
  1000 0.0009365081787109375
  10000 0.012650012969970703
  100000 0.15708255767822266
  1000000 1.703674077987671


Using mode probability: 1.0
 Using Tuples:
  1000 0.000827789306640625
  10000 0.011489391326904297
  100000 0.14312171936035156
  1000000 1.5596263408660889

 Using Concatenation:
  1000 0.0009191036224365234
  10000 0.012852668762207031
  100000 0.15607857704162598
  1000000 1.7298221588134766


 => The tuple version seems to be consistently faster by ~10%
"""
