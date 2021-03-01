<img src="docs/img/graphflow.png" height="181px" weight="377">

### Configuration GF-RV (row storage, volcano-styled processing)

Build steps
-----------------
* execute `./env.sh`
* To do a full clean build: `./gradlew clean build installDist`

Serializing Dataset
-----------------
* Dataset folder should have the following hierarchy:


    ├── vertices             # vertex files (one for `type`)  
    │   ├── v1.csv  
    │   └── ...  
    ├── edges                # edge files (one for each `label`)  
    │   ├── e1.csv  
    │   └── ...  
    └── metadata.json        # describes vertex and edge files.  

* `metadata.json` has the following format: 
```json
{
  "separator":  ",",
  "vertexFileDescriptions": [
    {
      "filename": "vertices/v1.csv",
      "type": "PERSON" 
    },
    ...
  ],
  "edgeFileDescriptions": [
    {
      "filename": "edges/e1.csv",
      "label": "KNOWS",
      "cardinality": "n-n"
    },
    ...
  ]
}
```
> `cardinality` can have one of these values: `"1-1"`, `"n-1"`, `"1-n"`, `"n-n"`

* Serialize as
```shell script
cd script/
[JAVA_OPTS="..."] python3 serailize_dataset.py <input-dataset> <output-directory>
```

Running Queries
-----------------
* Queries are written in Cypher query language and run using the Benchmark module. The Benchmark module takes as input the serialized dataset and a `.queries` file (which need to be placed in `benchmark/`), and reports the runtime (in msec), query plan and number of output tuples of each query in the file. 

* Queries file should be in the following format
```json
{
  "queries" : [
    {
      "name" : "sample-query",
      "query" : "MATCH (a:A)-[e:E]->(b:B) WHERE a.p1 > 12843 RETURN a.p2, b.p3",
      "execute" : "true",
      "planIdx" : 3
    }, 
    ....
  ]
}
```
> A particular query is executed only when `execute` is `true`.
> `planIdx` is the index of the query plan to be executed from the collection of all query plans that are generated for a particular query. If -1, all the query plans are executed.

* Run the benchmark module as
```shell script
cd script/
[JAVA_OPTS="..."] python3 benchmark.py -b <name-of-queries-file> -r <number-of-runs> -w <number-of-warmup-runs> <serialized-dataset-directory>
```
> Query plans of a query can be listed by setting `planIdx` to 1 and running the benchmark module with `-r` 0 and `-w` 0.



