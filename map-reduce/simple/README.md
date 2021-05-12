### Hadoop Map-Reduce Simple Program ###

1. Clone the project<br>


2. Go the project directory<br>
```cd hadoop-examples/map-reduce/simple```


3. Build the project<br>
```mvn -e clean package```


4. Delete the output directory if already exists<br>
```hdfs dfs -rm -r -skipTrash <output-directory>```


5. Copy the hadoop-examples/data/stocks-dataset.csv to the input directory<br>
```hdfs dfs -copyFromLocal hadoop-examples/data/stocks-dataset.csv  <input-directory>```


6. Submit the Map-Reduce job by running the jar as follows<br>
```hadoop jar simple-map-reduce-1.0-SNAPSHOT-jar-with-dependencies.jar <input-file-path> <output-directory>```


7. Check the output as follows<br>
```hdfs dfs -cat <output-directory>/*```