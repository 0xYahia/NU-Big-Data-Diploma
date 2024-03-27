# Introduction to Big Data
## LAB 1

### Steps to run jar file in Hadoop

#### Step 1: Create a Java Project
- Write a MapReduce program
- build the artifact jar file

#### Step 2: Copy the jar file, and your files to Hadoop
- docker cp <jar file> <container_id>:<path>
**Example:**
```bash
docker cp CIT650-Lab1-Project.jar hadoop:/home
docker cp task_data.txt hadoop:/home
```

- hdfs dfs -mkdir <folder name> -> create a folder in HDFS
- hdfs dfs -put <your fils> /folder-name -> copy the jar file to HDFS
**Example:**
```bash
hdfs dfs -mkdir /lab1-v2
hdfs dfs -put task_data.txt /lab1-v2
```

#### Step 3: Run the jar file
- hadoop jar <jar file> <main class> <input file> <output file>
**Example:**
```bash
hadoop jar CIT650-Lab1-Project.jar /lab1-v3/task_data.txt lab1-v3/out
```

#### Step 4: Check the output
- hdfs dfs -cat <output file> -> print the output file
**Example:**
```bash
hdfs dfs -ls lab1-v3/out -> list the output files
hadoop jar CIT650-Lab1-Project.jar  /lab1-v2/task_data.txt lab1-v2/out
hdfs dfs -cat lab1-v2/out/part-r-00000
```

#### Step 5: Copy the output file from Hadoop to the container
- hdfs dfs -get <output file> <local path>
**Example:**
```bash
hdfs dfs -get lab1-v3/out/part-r-00000 /home
```

#### Step 6: Copy the output file from the container to the local machine
- leave the container and run the following command
- docker cp <container_id>:<path> <local path>

**Example:**
```bash
docker cp hadoop:/home/part-r-00000 .
```