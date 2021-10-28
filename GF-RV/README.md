<img src="docs/img/graphflow.png" height="181px" weight="377">

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.com/graphflow/graphflow-core.svg?token=sBsSbpiSo2Uis6z98Ehs&branch=master)](https://travis-ci.org/graphflow/graphflow)
[![Coverage Status](https://coveralls.io/repos/github/graphflow/graphflow-core/badge.svg?branch=master&t=Hv91VR)](https://coveralls.io/github/graphflow/graphflow-core?branch=master)

Build steps
-----------------
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
    }
  ],
  "edgeFileDescriptions": [
    {
      "filename": "edges/e1.csv",
      "label": "KNOWS",
      "cardinality": "n-n"
    }
  ]
}
```
> `cardinality` can have one of these values: `"1-1"`, `"n-1"`, `"1-n"`, `"n-n"`

* Serialize as
```shell script
cd script/
[JAVA_OPTS="..."] python3 serailize_dataset.py <input-dataset> <output-directory>
```
> Recommeneded on himrod: `JAVA_OPTS="-Xmx475g -Xms475g -XX:+PrintGCDetails -XX:ParallelGCThreads=8"`

Contact
-----------------
[Amine Mhedhbi](http://amine.io/)
