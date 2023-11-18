package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<Text, // Input key type
        FloatWritable, // Input value type
        Text, // Output key type
        FloatWritable> { // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<FloatWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        System.out.println("reducer");
        float counter = 0;
        float tot = 0;
        Vector<Float> vec = new Vector<Float>();

        values.iterator().forEachRemaining(x -> vec.add(new Float(x.toString())));

        for (Float value : vec) {
            counter += 1;
            tot += value;
        }

        float avg = tot / counter;

        context.write(key, new FloatWritable(avg));

    }
}
