
# Get the project working!

## Contents

  * [Building](#building)
  * [Creating own dataset](#creating-own-dataset)
  * [Serializing the dataset](#serailizing-the-dataset)
  * [Running the benchmarking tool](#running-the-benchmarking-tool)

## Building 
We use Gradle 4.8.1 to manage builds. 

To do a full clean build: 

```shell script
./gradlew clean build installDist
```

Ensure that the `GRAPHFLOW_HOME` env variable is set from the root of the project.

```shell script
source env.sh
```

## Creating own dataset

GrahflowDB takes as parameter the directory that contains several CSV files. There are 2 types of files: 1) *Vertex files* that contains a vertex per line in file and 2) *Edge files* containing an edge between two vertices. Each vertex/edge file corresponds to one vertex/edge label in the graph to load. 

Dataset folder should have the following hierarchy:

    .
    ├── vertices                # All vertex files goes in here
    │   ├── v1.csv
    │   ├── v2.csv         
    │   └── ...
    ├── edges                   # All edge files goes in here
    │   ├── e1.csv
    │   ├── e2.csv         
    │   └── ...
    └── metadata.json

The `metadata.json` maps the vertex/edge file to its corresponding label. It also defines other important values per edge label, `cardinality` and `storeCompressed` that instructs the system to leverage columnar storage and compression features wherever possible.

`metadata.json` has the following format: 
```json
{
  "separator":  ",",
  "vertexFileDescriptions": [
    {
      "filename": "vertices/v1.csv",
      "type": "PERSON" 
    }
  ],
  "edgeFileDescriptions": [
    {
      "filename": "edges/e1.csv",
      "label": "KNOWS",
      "cardinality": "n-n"
      "storeCompressed": [true, false]
    }
  ]
}
```
> `cardinality` can have one of these values: `"1-1"`, `"n-1"`, `"1-n"`, `"n-n"` based on the relationship cardinality of the edge label.
> `storeCompressed` indicates if the forward and backward adjacency list should be compressed. This is beneficial for relationships where the forward or backword cardinality can alse be 0. 

### Vertex file
The first line is the header which specifies the vertex ID (strictly first) followed by property names with one of the valid datatype. Valid datatypes are `INT`, `DOUBLE`, `STRING` and `BOOLEAN`. Vertex id has a fixed type `NODE`. Each subsequent line is a vertex in graph data along with its properties or `NULL` value.

Example file
```csv
id:NODE,name:STRING,country_code:STRING,imdb_id:STRING,name_pcode_nf:STRING,name_pcode_sf:STRING,md5sum:STRING
6695803,Comfilm.de,[de],,C5145,C5145,103fec65d8a8129bfea07e4a98dfa0e5
6695804,Dusty Nose Productions,[us],,D2352,D2352,7a9718e93925d720de807fe027665fbe
6695805,WTTW National Productions,[us],,W3535,W3535,002eb2b89338ca14386743e11e7a425f
...
```

### Edge file
Similar organization and header as that of the vertex file. Only difference is that each line now contains reference to 2 vertex ID: one denotes the origin of the edge while other is the end.  

Example file
```csv
from:NODE,to:NODE,company_type_id:INT,note:STRING
161883,6699143,1,(2006) (USA) (TV)
161883,6699143,1,(2006) (worldwide) (TV)
125713,6730924,1,(2012) (worldwide) (all media)
141089,6739905,1,(2013) (USA) (all media)
...
...
```


## Serailizing the dataset

Once the dataset has been prepared. it needs to be serialized to the native-Graphflow format. The serialized data can be loaded to the system efficiently each time the query has to be benchmarked.

To serialize the dataset from `GRAPHFLOW_HOME`

```shell script
cd script/
python3 serialize_dataset.py <input-dataset> <serialized-directory>
```
### Requiring More Memory?

Note that the JVM heap by default is allocated a max of 2GB of memory. Changing the JVM heap maximum size can be done by prepending `JAVA_OPTS` when calling runner python scripts. 

Recommeneded for large datasets: `JAVA_OPTS="-Xmx475g -Xms475g -XX:ParallelGCThreads=8"`

Usage
```
JAVA_OPTS="..." python3 serialize_dataset.py <input-dataset> <serialized-directory>
```

## Running the Benchmarking tool

To benchmark a query in Graphflow, the cypher query needs to be organized in a `.query` file. This file also contains a *Query vertex ordering* that guides the planner to produce the query plan that has the required join order. 

The `.query` file needs to be kept in the `benchmark/` folder in `GRAPHFLOW_HOME`.

`.query` file is written in JSON and has the following format.

```json
{
  "name": "<name>",
  "qstr": "<query_string>",
  "qvo": [
    "node_variable_1",
    "node_variable_2",
    "..."
  ]
}
```

To execute the benchmark tool
```shell script
cd script/
[JAVA_OPTS="..."] python3 benchmark.py -w <num_warmup_runs> -r <num_runs> <serialized_directory> <query_filename>
```

> **Recommended:** Execute queries in warmup for 2-3 runs to normalize any cache behaviour. 

> `query_filename` is the name of the `.query` file without the extension.
