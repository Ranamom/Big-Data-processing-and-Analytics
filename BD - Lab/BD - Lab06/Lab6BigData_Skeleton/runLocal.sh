# Remove folders of the previous run
rm -rf out_Lab6

# Run application
spark-submit  --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master local  target/Lab6_Skeleton-1.0.0.jar ReviewsSample.csv out_Lab6



