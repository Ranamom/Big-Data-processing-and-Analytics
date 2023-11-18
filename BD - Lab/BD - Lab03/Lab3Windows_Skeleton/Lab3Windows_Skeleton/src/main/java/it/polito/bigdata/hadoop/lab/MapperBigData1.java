package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> {// Output value type

    protected void map(
            LongWritable key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split(",");
        String[] products = Arrays.copyOfRange(words, 1, words.length);

        for (int i = 0; i < products.length; i++) {
            for (int j = i + 1; j < products.length; j++) {
                if (!products[i].equals(products[j])) {

                    // I impose always the same order also for different couple composed by same
                    // elements

                    String min = products[i].compareTo(products[j]) < 0 ? products[i] : products[j];
                    String max = products[i].compareTo(products[j]) >= 0 ? products[i] : products[j];

                    context.write(new Text(min + "," + max), new IntWritable(1)); // ("A,B",1)
                }

            }
        }
    }
}
