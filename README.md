# Advanced Database Systems project - ArcadeDB + GeoPandas

## Aim of this project:
The aim of this project was to create a geospatial database system using ArcadeDB for data storage and GeoPandas for data processing. 

The data was obtained from my professor and centered around the administrative division of Poland and it's neighbours.

It consisted of the following objects with geometries, which were stored in csv files, included in the repository:
- countries
- voivodeships
- powiats
- communes
- cities

and also data not included in the repository:
- roads
- railroads 
- trees 
- buildings

The overall pipeline of the project was as follows:
1. Import the data into the database from csv files
   - create database schema
   - split big csv files into smaller ones
   - read each csv file with geopandas and convert each row into a sql insert statement
   - run the insert statements in the database via the ArcadeDB console
2. Create relationships between imported objects
   - import appropriate csv files for each relationship
   - compute the relationships between the objects using geopandas
   - create insert edge statements for each relationship
   - run the insert edge statements in the database via the ArcadeDB console
3. Run some geospatial queries to eg. find the number of cities within each commune, adjacent powiats and adjacent voivodships.
   - manually run the queries in the ArcadeDB console

Throught this project I encountered numerous problems with ArcadeDB, mostly with the console tool:
1. it does not support output of queries ran from a subprocess
2. it does not support multiline queries
3. inside the console tool there is a character limit in the CSV Importer set to 4096 characters, that is impossible to change
4. it often throws Java heap space error, which is hard to resolve
5. it often locks the database, which requires to restart the database, or a buggy workaround with the console tool

There were also some problems with the database itself:
1. no support of Python API
2. support of geometries is limited to analyzing them, not storing them
3. the database is not very efficient in terms of memory usage
4. the database is not very efficient in terms of execution time, especially edge creation

All in all, the project was a great learning experience, as I learned a lot about geospatial databases, geospatial data processing and the problems that come with it.
I highly discourage using ArcadeDB for any serious project, as it is not very stable and has a lot of limitations.


## How to start clean container (on WSL):

1. clone this repository
2. from the root of the repository run:
```bash
docker compose up --detach --build --no-deps arcade
```
3. get container_id via (usually it is <ads_project-arcade-1>):
```bash
docker ps
```
4. log into docker bash:
```bash
docker exec -it ads_project-arcade-1 /bin/bash
```
5. from the container bash run:
```bash
cd /home/arcadedb
python3 scripts/import_setup.py
python3 scripts/import_data.py
```
6. to import relationships:
```bash
python3 scripts/import_relation.py <relation_number>
```
7the logged times should be located in:
```txt
/home/arcadedb/logs/
```
8. to run the queries see the Queries section below


## Queries
To run queries and see the results we can't actually use the ArcadeDB console, as it does not support output of queries ran from a subprocess.
This means you need to actually run the queries from the console one by one, by hand.

Note: you need the query to be in one line, as **the ArcadeDB console tool does not support multiline queries.**

To run the query:
1. from the container bash (see above steps 2-4), run the console:
```bash
/home/arcadedb/bin/console.sh
```
2. connect to database (sadly because of locking issues you need to pass the password every time you connect to the database)
```bash
connect remote:localhost/ads24_db root rootroot
```
3. set the language to cypher
```bash
set language = cypher
```
4. and now you can run the queries as shown below:

### Query 1: Number of cities within each commune
```bash
MATCH (city:region{type:"city"})-[:within]->(commune:region{type:"commune"}) RETURN commune.name AS Commune, COUNT(city) AS NumberOfCities;
```

### Query 2: Adjacent powiats
```bash
MATCH (commune1:region{type:"commune"})-[:neighbour ]-(commune2:region{type:"commune"}) MATCH (commune1)-[:within]->(powiat1:region{type:"powiat"}) MATCH (commune2)-[:within]->(powiat2:region{type:"powiat"}) WHERE powiat1 <> powiat2 RETURN DISTINCT powiat1.name AS Powiat1, powiat2.name AS Powiat2;
```

#### Query 3: Adjacent voivodships
```bash
MATCH (commune1:region{type:"commune"})-[:neighbour]-(commune2:region{type:"commune"}) MATCH (commune1)-[:within]->(powiat1:region{type:"powiat"})-[:within]->(voivodeship1:region{type:"voivodeship"}) MATCH (commune2)-[:within]->(powiat2:region{type:"powiat"})-[:within]->(voivodeship2:region{type:"voivodeship"}) WHERE voivodeship1 <> voivodeship2 RETURN DISTINCT voivodeship1.name AS Voivodeship1, voivodeship2.name AS Voivodeship2;
```

