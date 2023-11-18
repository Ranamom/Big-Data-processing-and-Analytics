package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tools.ant.taskdefs.condition.HasMethod;

/**
 * Lab - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        FloatWritable> {// Output value type

    HashMap<String, ArrayList<String>> statistics; // UserId -> <ProductId, Score>

    protected void setup(Context context) {
        statistics = new HashMap<String, ArrayList<String>>();
    }

    protected void map(
            LongWritable key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {

        ArrayList<String> prod_list;
        String[] fields = value.toString().split(",");

        if (fields[0] != "Id") { // the first line (header) is not to consider

            String productId = fields[1];

            String userId = fields[2];
            int score = Integer.parseInt(fields[8]);
            prod_list = statistics.get(userId);

            if (prod_list == null) {
                prod_list = new ArrayList<String>();
                prod_list.add(productId + ":" + score);
                statistics.put(userId, prod_list);
            } else {
                prod_list.add(productId + ":" + score);
            }

        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Emit the set of (key, value) pairs of this mapper

        // for each user
        for (Entry<String, ArrayList<String>> pair : statistics.entrySet()) {
            ArrayList<String> vec = pair.getValue();

            // you need a sum and a len to compute the avg along the column
            int tot = 0;
            int counter = vec.size();

            for (String id_and_score : vec) {
                String[] fields = id_and_score.split(":");
                tot += Integer.parseInt(fields[1]);
            }

            // compute the mean value for each column (each user)
            float mean = tot / counter;

            // apply the normalization to each value and then send the pair
            for (String id_and_score : vec) {

                String[] fields = id_and_score.split(":");
                float norm_value = Float.parseFloat(fields[1]) - mean;

                // I sent a pair (product_Id,normalized_score) for each product
                context.write(new Text(fields[0]),
                        new FloatWritable(norm_value));
            }
        }

    }
}
