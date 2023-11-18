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

        // System.out.println("Mapper");
        ArrayList<String> prod_list;
        String[] fields = value.toString().split(",");
        // System.out.println("Fields: ");
        // for (String x : fields) {
        // System.out.println(x);
        // }

        if (!fields[0].equals("Id")) { // the first line (header) is not to consider

            String productId = fields[1];
            String userId = fields[2];
            // System.out.println("prodid: " + productId);
            // System.out.println("userId: " + userId);
            int score = Integer.parseInt(fields[6]);
            prod_list = statistics.get(userId);
            // System.out.println("Dopo prod_list.get");
            if (prod_list == null) {
                // System.out.println("in prod_listt == null");
                prod_list = new ArrayList<String>();
                prod_list.add(productId + ":" + score);
                statistics.put(userId, prod_list);
                // System.out.println("fine prod_list == null");
            } else {
                // System.out.println("in prod_list != null");
                prod_list.add(productId + ":" + score);
            }

            // System.out.println(statistics);

        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Emit the set of (key, value) pairs of this mapper
        // System.out.println("nel cleanup");
        // for each user
        for (Entry<String, ArrayList<String>> pair : statistics.entrySet()) {
            ArrayList<String> vec = pair.getValue();
            // System.out.println("nel for del cleanup");
            // you need a sum and a len to compute the avg along the column
            int tot = 0;
            int counter = vec.size();
            System.out.println(vec);
            for (String id_and_score : vec) {
                System.out.println(id_and_score);
                String[] fields = id_and_score.split(":");
                tot += Integer.parseInt(fields[1]);
                System.out.println(tot);
            }

            // compute the mean value for each column (each user)
            float mean = new Float(tot) / new Float(counter);
            System.out.println(mean);
            // apply the normalization to each value and then send the pair
            for (String id_and_score : vec) {
                // System.out.println("nel secodno for pi+ interno del cleanup");
                String[] fields = id_and_score.split(":");
                float norm_value = Float.parseFloat(fields[1]) - mean;
                System.out.println(norm_value);
                // I sent a pair (product_Id,normalized_score) for each product
                context.write(new Text(fields[0]),
                        new FloatWritable(norm_value));
            }
        }

    }
}
