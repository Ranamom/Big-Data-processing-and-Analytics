package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> {// Output value type

    protected void map(
            LongWritable key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {

        // Extract words and occurruencies from the key
        // you have to look into value because we have set a
        // job.setInputFormatClass(TextInputFormat.class) in the driver
        String[] fields = value.toString().split("\\s+");

        String word = fields[0];
        String occurr = fields[1];
        String starter = new String("ho");
        if (word.startsWith(starter)) {
            context.write(new Text(word), new IntWritable(new Integer(occurr)));
        }
    }
}
