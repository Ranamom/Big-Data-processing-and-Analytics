# Remove folders of the previous run
rm -rf out_Lab5

# Run application
spark-submit  --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master local  target/Lab5_Template-1.0.0.jar SampleLocalFile.csv out_Lab5/ "ho"



