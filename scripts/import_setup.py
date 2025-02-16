import subprocess
import os
from itertools import islice
from tqdm import tqdm
import gc

init_sql_strings = [
    'CREATE DATABASE ads24_db;'
    'CREATE VERTEX TYPE node;',
    'CREATE PROPERTY node.type STRING (mandatory true, notnull true);',
    'CREATE VERTEX TYPE region EXTENDS node;',
    'CREATE PROPERTY region.name STRING (mandatory true, notnull true);',
    'CREATE VERTEX TYPE building EXTENDS node;',
    'CREATE PROPERTY building.subtype STRING (mandatory true);',
    'CREATE VERTEX TYPE railway EXTENDS node;',
    'CREATE PROPERTY railway.subtype STRING;',
    'CREATE VERTEX TYPE tree EXTENDS node;',
    'CREATE PROPERTY tree.id STRING (mandatory true);',
    'CREATE VERTEX TYPE road EXTENDS node;',
    'CREATE PROPERTY road.name STRING (mandatory true);',
    'CREATE PROPERTY road.class STRING;',
    'CREATE PROPERTY road.nodes STRING;'
    'CREATE EDGE TYPE within;',
    'CREATE EDGE TYPE neighbour;',
    'exit;'
]

init_sql_string = " ".join(init_sql_strings).replace("\n", "")
with open("/home/arcadedb/data/init.sql", "w", encoding='utf-8') as f:
    f.write(init_sql_string)
subprocess.run(["/home/arcadedb/bin/console.sh", "-b", "load /home/arcadedb/data/init.sql"])

if not os.path.exists("/home/arcadedb/logs"):
    os.mkdir("/home/arcadedb/logs")

# split ads24-buildings.csv into 6 parts, every 3 million lines
with open("/home/arcadedb/data/ads24-buildings.csv", "r", encoding='utf-8') as f:
    header = f.readline()
    for i, sli in tqdm(enumerate(iter(lambda: list(islice(f, 3000000)), []))):
        with open(f"/home/arcadedb/data/ads24-buildings_{i}.csv", "w", encoding='utf-8') as f2:
            f2.write(header)
            f2.writelines(sli)
gc.collect()
