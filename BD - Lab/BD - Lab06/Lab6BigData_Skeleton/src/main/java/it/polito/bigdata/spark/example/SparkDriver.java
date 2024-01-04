package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.List;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #6").setMaster("local");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		// Remember to remove .setMaster("local") before running your application on the
		// cluster

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		LongAccumulator longAcc = sc.sc().longAccumulator();

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: " + JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");

		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath).filter(x -> !x.equals(
				"Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,Score,Time,Summary,Text"));

		// Create PairRDDs containing User,user_bought_product
		JavaPairRDD<String, String> pairRDD = inputRDD.mapToPair(
				x -> {

					Tuple2<String, String> pair;
					String[] fields = x.split(",");
					String userId = fields[2];
					String productId = fields[1];
					pair = new Tuple2<String, String>(userId, productId);
					return pair;
				}).distinct(); // the track requests distinct values of the productId for each UserID

		// Transform all these pairs in pairs (user,list_of_bought_products)
		JavaPairRDD<String, Iterable<String>> listedRDD = pairRDD.groupByKey();
		// listedRDD.foreach(x -> System.out.println(x));

		JavaPairRDD<String, List<Tuple2<String, String>>> pairedProductsRDD = listedRDD
				.mapToPair(listRDD -> {
					List<Tuple2<String, String>> pairs = new ArrayList<>();
					List<String> products = new ArrayList<>();

					// add each product to the user's products list
					for (String p : listRDD._2) {

						products.add(p);
					}

					for (int i = 0; i < products.size(); i++) {
						for (int j = i + 1; j < products.size(); j++) {
							String e1 = products.get(i);
							String e2 = products.get(j);
							if (e1.compareTo(e2) > 0) {
								String tmp = e1;
								e1 = e2;
								e2 = tmp;
							}
							Tuple2<String, String> pair = new Tuple2<String, String>(e1, e2);
							pairs.add(pair);
						}
					}

					return new Tuple2<String, List<Tuple2<String, String>>>(listRDD._1, pairs);
				});

		// Now take only the values (i.e. the product pairs) and create an RDD with them
		JavaRDD<Tuple2<String, String>> pairsRDD = pairedProductsRDD.filter(x -> x._2.size() > 0).values()
				.flatMap(lista -> lista.iterator());

		// create a new PairRDD with the previous pairs and +1 as value for each of them
		JavaPairRDD<Tuple2<String, String>, Integer> pairsAccRDD = pairsRDD.mapToPair(pair -> {
			return new Tuple2<Tuple2<String, String>, Integer>(pair, 1);
		});

		// sum all the contributes of each pair
		JavaPairRDD<Tuple2<String, String>, Integer> reducedRDD = pairsAccRDD.reduceByKey((value1, value2) -> {
			return value1 + value2;
		});

		// reducedRDD.foreach(x -> System.out.println(x));

		// filter the list to only mantain pairs with frequency bigger than 1 and order
		// than in descending way
		JavaPairRDD<Integer, Tuple2<String, String>> filtOrdRDD = reducedRDD.filter(pair -> pair._2 > 1)
				.mapToPair(x -> {
					return new Tuple2<Integer, Tuple2<String, String>>(x._2, x._1);
				})
				.sortByKey(false);
		filtOrdRDD.saveAsTextFile(outputPath);
		sc.close();
	}
}
