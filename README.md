This project utilizes Hadoop MapReduce and Apache Spark for data processing tasks. It includes a set of Java files for MapReduce jobs and a Python script for data cleaning using Spark.

Dataset: AppleAppSore.csv (https://www.kaggle.com/datasets/gauthamp10/apple-appstore-apps/data)

Files Included:
1. Jar File: This contains compiled Java classes for MapReduce jobs.
	1. words-1.0-SNAPSHOT.jar


2. Java Files: These are the source code files for MapReduce jobs.
	1. AppPerDeveloper.java
	2. AppRatingCorrelation.java
	3. FreePaidApp.java
	4. SizeAndRating.java
	5. AvgRatingAppUpdates.java


3. Data Cleaning Python Script: A Python script for data cleaning implemented using Apache Spark.
	1. CleanData.py


Running Data Cleaning
To perform data cleaning using the Python script with Spark:
1. Ensure you have Apache Spark installed and configured.
2. Execute the Python script with the following command:
spark-submit <data_cleaning_script.py> <input_path> <output_path>
Replace <data_cleaning_script.py> with the path to the Python script, <input_path> with the path to the input data, and <output_path> with the desired output path.


Running MapReduce Jobs
To run the MapReduce jobs, follow these steps:
1. Ensure you have Hadoop installed and configured properly.
2. Upload input data to the Hadoop Distributed File System (HDFS).
 hadoop fs -put <local_path_of_file> <output_path>
3. Create the jar file of the MapReduce Job using any IDE.
4. Use the following command to execute a MapReduce job:
hadoop jar <path_to_jar_file.jar> <Main_Class_Name> <input_path> <output_path>
5. To view the output execute the following command:
hadoop fs -cat <output_path>


Dependencies
* Hadoop
* Apache Spark
