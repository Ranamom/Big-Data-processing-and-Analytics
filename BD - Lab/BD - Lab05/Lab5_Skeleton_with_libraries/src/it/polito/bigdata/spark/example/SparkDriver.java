package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

import java.util.List;

import javax.swing.text.html.HTMLDocument.Iterator;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages

		String inputPath;
		String outputPath;
		String prefix;

		inputPath = args[0];
		outputPath = args[1];
		prefix = args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #5"); // remember to delete the
																		// setMaster command before
																		// launching it on a Jupyter
																		// cluster because that's only
																		// necessary to locally run your
																		// application

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the
		// cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: " + JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");

		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data
		// (i.e, one pair "word\tfreq")

		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);
		// System.out.println(wordFreqRDD.count());

		// ** TASK 1 **//
		JavaRDD<String> firstFilter = wordFreqRDD.filter(p -> p.toLowerCase().contains(prefix));
		// System.out.println("firstFilters elements: " + firstFilter.count());

		JavaRDD<Integer> freq = firstFilter.map(p -> {

			String[] parts = p.split("\\t");
			Integer occ = Integer.parseInt(parts[1]);
			return occ;
		});

		Long numLines = freq.count();
		System.out.println("Number of lines containing the element " + prefix + " : " + numLines);
		Integer maxFreq = freq.top(1).get(0);
		System.out.println("Higher frequency: " + maxFreq);

		// ** TASK2 **//

		JavaRDD<String> secondFilter = firstFilter.filter(p -> {
			String[] parts = p.split("\\t");
			Integer occ = Integer.parseInt(parts[1]);
			if (occ.floatValue() / maxFreq.floatValue() > 0.8) {
				return true;
			} else {
				return false;
			}
		});

		JavaRDD<String> freq2 = secondFilter.map(p -> {
			String[] parts = p.split("\\t");
			String name = parts[0];
			return name;
		});

		Long numLines2 = freq2.count();
		System.out.println("Number of lines respecting the second filter the element:" + numLines2);

		// Store the result in the output folder
		freq2.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();

	}
}
