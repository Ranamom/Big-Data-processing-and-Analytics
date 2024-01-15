# Remove output folder of the previous run
hdfs dfs -rm -r out_Lab7


# Run application
spark-submit  --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode cluster --master yarn  target/Lab7_Template-1.0.0.jar /data/students/bigdata-01QYD/Lab7/register.csv /data/students/bigdata-01QYD/Lab7/stations.csv 0.6 out_Lab7
