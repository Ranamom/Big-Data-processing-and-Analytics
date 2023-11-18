# Remove folders of the previous run
hdfs dfs -rm -r example_data
hdfs dfs -rm -r example_out

# Put input data collection into hdfs
hdfs dfs -put example_data


# Run application
hadoop jar MapReduceProject-1.0.0.jar it.polito.bigdata.hadoop.DriverBigData 2 example_data/  example_out




