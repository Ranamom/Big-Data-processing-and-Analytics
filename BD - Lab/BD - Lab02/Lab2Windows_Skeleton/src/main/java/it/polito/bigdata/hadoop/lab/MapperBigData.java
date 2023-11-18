package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.lab.DriverBigData.MY_COUNTERS;

/**
 * Lab - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> {// Output value type

    String starter;

    protected void setup(Context context) {
        // I retrieve the value of the threshold only one time for each mapper
        starter = context.getConfiguration().get("starter");
        // for the second point of the lab we want to use a Counter Pattern
    }

    protected void map(
            LongWritable key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {

        // Extract words and occurruencies from the key
        String[] fields = value.toString().split("\\s+");
        // System.out.println(fields.length);
        // System.out.println(fields[0]);
        // System.out.println(fields[1]);

        String word = fields[0];
        String occurr = fields[1];

        if (word.startsWith(starter)) {
            context.write(new Text(word), new IntWritable(new Integer(occurr)));
            context.getCounter(MY_COUNTERS.SELECTED).increment(1);
        } else {
            context.getCounter(MY_COUNTERS.DISCARDED).increment(1);
        }
    }
}
