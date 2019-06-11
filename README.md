# Wikipedia-Index

### Introduction
* Project of distributed system course.A distributed system of Wikipedia Index.

### Usage
**Connect to the cluster or run on the ubuntu installed hadoop.
After start the hdfs and yarn:**

get the term-frequency
```
$ hadoop jar WikiIndex.jar TF {inputPath} {outputPath}
```

get the document-frequency
```
$ hadoop jar WikiIndex.jar DF {inputPath} {outputPath}
```

get the position of every term
```
$ hadoop jar WikiIndex.jar Position {inputPath} {outputPath}
```

get the TF,DF,Position
```
$ hadoop jar WikiIndex.jar TF_Position {inputPath} {outputPath1}
$ hadoop jar WikiIndex.jar TF_DF_Position {outputPath1} {outputPath2}
```

*{inputPath} is the path of Wikipedia corpus.
 {outputPath} is the path of save files.*

### Visualization
* Just realize the local visualization. Details see the report above.
