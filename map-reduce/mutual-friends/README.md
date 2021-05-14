### Hadoop Map-Reduce Program to find mutual friends ###

1. Clone the project<br>


2. Go the project directory<br>
```cd hadoop-examples/map-reduce/mutual-friends```


3. Build the project<br>
```mvn -e clean package```


4. Delete the output directory if already exists<br>
```hdfs dfs -rm -r -skipTrash <output-directory>```


5. Copy the hadoop-examples/data/stocks-dataset.csv to the input directory<br>
```hdfs dfs -copyFromLocal hadoop-examples/data/mutual-friends <input-directory>```


6. Submit the Map-Reduce job by running the jar as follows<br>
```hadoop jar mutual-friends-1.0-SNAPSHOT-jar-with-dependencies.jar <input-file-path> <output-directory>```


7. Check the output-directory is non empty as follows<br>
```hdfs dfs -ls <output-directory>/*```


8. Verify the output<br>
```hdfs dfs -libjars mutual-friends-1.0-SNAPSHOT-jar-with-dependencies.jar -text <output-directory>/*```


#### Additional Details ####
* Command to get default blocksize defined<br>
```hdfs getconf -confKey dfs.blocksize```


* Command to get blockszie of the file in the cluster<br>
```hadoop fs -stat %o <input-file-path>```


* Command to run mapreduce job with more mappers<br>
```hadoop jar mutual-friends-1.0-SNAPSHOT-jar-with-dependencies.jar -Dmapreduce.input.fileinputformat.split.minsize=<bytes> <input-file-path> <output-directory>```<br>
**If the bytes passed in split.minsize is less than actual blocksize of the file, then it'll run with default mappers count. Hence check the actual blocksize of the file before running this command**


* Command to run mapreduce job with more reducers<br>
```hadoop jar mutual-friends-1.0-SNAPSHOT-jar-with-dependencies.jar -Dmapreduce.job.reduces=<reducer-count> <input-file-path> <output-directory>```


* We can also pass both -Dmapreduce.input.fileinputformat.split.minsize and -Dmapreduce.job.reduces in the same command, but always make sure when passing those extra options, have those before actual arguments to the mapreduce job<br>
