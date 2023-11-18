package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Counter;

/**
 * MapReduce program
 */

/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

  public static enum MY_COUNTERS {
    SELECTED,
    DISCARDED
  }

  @Override
  public int run(String[] args) throws Exception {

    int exitCode = 0;

    Path inputPath;
    Path outputDir;
    int numberOfReducers;

    // Parse the parameters
    numberOfReducers = 0; // for a FILTER PATTERN no reducers are required
    inputPath = new Path(args[0]);
    outputDir = new Path(args[1]);

    Configuration conf = this.getConf();

    // be carefull to put this conf setup before the Job instanciation
    conf.set("starter", args[2]);

    // Define a new job
    Job job = Job.getInstance(conf);

    // Assign a name to the job
    job.setJobName("Lab - Skeleton");

    // Set path of the input file/folder (if it is a folder, the job reads all the
    // files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath);

    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);

    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);

    // Set job input format
    job.setInputFormatClass(TextInputFormat.class);

    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set map class
    job.setMapperClass(MapperBigData.class);

    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // Set reduce class
    // job.setReducerClass(ReducerBigData.class); //for a FILTER PATTERN no reducers
    // are required

    // Set reduce output key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set number of reducers -> for a FILTER PATTERN no reducers are required
    job.setNumReduceTasks(numberOfReducers);

    // Execute the job and wait for completion
    if (job.waitForCompletion(true) == true) {
      exitCode = 0;
      Counter selectedRecords = job.getCounters().findCounter(MY_COUNTERS.SELECTED);
      Counter discardedRecords = job.getCounters().findCounter(MY_COUNTERS.DISCARDED);

      System.out.println("Total number of selected records = " + selectedRecords.getValue());
      System.out.println("Total number of discarded records = " + discardedRecords.getValue());

    } else
      exitCode = 1;
    return exitCode;

  }

  /**
   * Main of the driver
   */

  public static void main(String args[]) throws Exception {
    // Exploit the ToolRunner class to "configure" and run the Hadoop application
    System.out.println(args[0]);
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);
    System.exit(res);
  }
}
