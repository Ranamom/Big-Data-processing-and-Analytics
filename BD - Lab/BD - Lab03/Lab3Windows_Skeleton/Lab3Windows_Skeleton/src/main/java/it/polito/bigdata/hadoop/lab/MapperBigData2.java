package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import it.polito.bigdata.hadoop.lab.WordCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        NullWritable, // Output key type
        WordCountWritable> {// Output value type

    protected void map(
            LongWritable key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the map method */

        String[] words = value.toString().split("\\s+");
        String pair = words[0];
        String occ = words[1];
        WordCountWritable wc = new WordCountWritable(pair, new Integer(occ));
        context.write(NullWritable.get(), wc);

    }
}
