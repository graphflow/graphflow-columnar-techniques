<img src="docs/img/graphflow.png" height="181px" weight="377">

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

All code can be found under src/

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

Workloads to get started
------------------------
[JOB Queries & CSV](https://drive.google.com/file/d/1cy6XBgle9_18hPXsNgd1IDgctNG87DQu/view?usp=sharing)    
[LDBC Queries](https://drive.google.com/file/d/1jcU9GeX4UXdGRom_Mf5UP2I2Iu4w-YfB/view?usp=sharing)

Contact
-----------------
[Pranjal Gupta](https://g31pranjal.github.io/)   
[Amine Mhedhbi](http://amine.io/)

