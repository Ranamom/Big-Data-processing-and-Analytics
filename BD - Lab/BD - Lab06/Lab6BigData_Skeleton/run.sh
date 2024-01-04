# Remove output folder of the previous run
hdfs dfs -rm -r out_Lab6

# Run application
spark-submit  --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master yarn  target/Lab6_Skeleton-1.0.0.jar /data/students/bigdata-01QYD/Lab4/Reviews.csv out_Lab6/ 

