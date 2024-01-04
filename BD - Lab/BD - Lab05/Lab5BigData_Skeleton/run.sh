# Remove output folder of the previous run
hdfs dfs -rm -r out_Lab5

# Run application
spark-submit  --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master yarn  Lab5_Template-1.0.0.jar /data/students/bigdata-01QYD/Lab2/ out_Lab5/ "ho"

