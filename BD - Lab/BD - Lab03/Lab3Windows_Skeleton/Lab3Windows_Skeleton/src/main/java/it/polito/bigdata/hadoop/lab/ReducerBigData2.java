package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import it.polito.bigdata.hadoop.lab.TopKVector;
import it.polito.bigdata.hadoop.lab.WordCountWritable;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<NullWritable, // Input key type
        WordCountWritable, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

    @Override
    protected void reduce(
            NullWritable key, // Input key type
            Iterable<WordCountWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        System.out.println("Avvio reducer");
        TopKVector<WordCountWritable> topk = new TopKVector<WordCountWritable>(100);
        // int count = 0;
        // for (WordCountWritable v : values) {
        // count++;
        // }
        // System.out.println("Dimensione values: " + count); //

        for (WordCountWritable v : values) {
            WordCountWritable copy = new WordCountWritable(v.getWord(), v.getCount());
            topk.updateWithNewElement(copy);
        }

        Vector<WordCountWritable> top_values = topk.getLocalTopK();

        for (WordCountWritable v : top_values) {
            context.write(new Text(v.getWord()), new IntWritable(v.getCount()));
        }

    }
}
