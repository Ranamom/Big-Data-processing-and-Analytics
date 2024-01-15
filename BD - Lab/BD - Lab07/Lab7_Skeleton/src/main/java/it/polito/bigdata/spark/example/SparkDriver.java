package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.json4s.DefaultWriters.W;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: " + JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");

		// Read the content of the readings
		JavaRDD<String> readingsRDD = sc.textFile(inputPath);

		// filter the lines with 0 free slots and 0 busy slots error
		JavaRDD<String> filteredRegisterRDD = readingsRDD.filter(line -> {
			String[] fields = line.split("\\t");

			// remove the header
			if (fields[0].equals("station"))
				return false;

			// check if the station has 0 free slots and 0 busy slots
			if (Integer.parseInt(fields[2]) == 0 && Integer.parseInt(fields[3]) == 0)
				return false;
			else
				return true;
		});

		// Read the stations file and mantain only stationId and coordinated
		// remove the header before

		JavaRDD<String> stationsRDD = sc.textFile(inputPath2).filter(line -> {
			String[] fields = line.split("\t");
			if (fields[0].equals("id"))
				return false;
			else
				return true;
		}).map(line -> {
			String[] fields = line.split("\t");
			// System.out.println(fields[3]);
			return fields[0] + "\t" + fields[1] + "\t" + fields[2];

		});

		// Create a pair RDD (stationId, coordinates)
		JavaPairRDD<String, String> stationsPairRDD = stationsRDD.mapToPair(line -> {

			String[] fields = line.split("\t");
			if (fields.length < 3)
				return new Tuple2<>(fields[0], "?" + "\t" + "?");
			// System.out.println(line);
			return new Tuple2<>(fields[0], fields[1] + "\t" + fields[2]);
		});

		// Task1 : Identifies the most “critical” timeslot for each station.
		// The “criticality” of a station Si in the timeslot Tj is defined as (total
		// number of readings with number of free slot equal to 0 for the pair
		// (Si,Tj)/total number of readings for the pair (Si,Tj))
		// each combination “day of the week – hour” is a timeslot and is associated
		// with all the readings associated with that combination, independently of the
		// date. For
		// instance, the timeslot “Wednesday - 15” corresponds to all the readings made
		// on
		// Wednesday from 15:00:00 to 15:59:59, independently of the date.

		// Generate pairs (stationId, criticality) -> I would suggest a groupByKey
		// and a reduceByKey transformation

		// Refactor the registerRDD to have the following format:
		// stationId \t slot \t freeSlots \t busySlots
		JavaRDD<String> refactoredRegisterRDD = filteredRegisterRDD.map(line -> {
			String[] fields = line.split("\\t");

			String time = fields[1]; // Assuming the time field is stored in fields[1]
			String[] timeParts = time.split(" ");
			String day = timeParts[0];
			String hour = timeParts[1].split(":")[0];

			// Each slot is a concatenation of day of the week and the hour slot
			String slot = DateTool.DayOfTheWeek(day) + " - " + hour;

			return fields[0] + "\t" + slot + "\t" + fields[2] + "\t" + fields[3];

		});

		// Generate pairs (stationId, (freeSlots, busySlots))
		JavaPairRDD<String, Tuple2<Integer, Integer>> pairRecordRDD = refactoredRegisterRDD.mapToPair(line -> {
			String[] fields = line.split("\t");
			String key = fields[0] + "\t" + fields[1];
			Tuple2<Integer, Integer> value = new Tuple2<>(Integer.parseInt(fields[2]), Integer.parseInt(fields[3]));

			return new Tuple2<String, Tuple2<Integer, Integer>>(key, value);
		});

		// Reduce the pairs by key and sum the number of free slots for a specific
		// station over all the instances of a slot
		JavaPairRDD<String, Tuple2<Integer, Integer>> reducedRecordRDD = pairRecordRDD.reduceByKey((v1, v2) -> {
			return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
		});
		JavaPairRDD<String, Integer> freeSlotRDD = reducedRecordRDD.mapValues(value -> value._1);

		// Count the number of readings for each slot related to a specific station
		JavaPairRDD<String, Integer> countSlotRDD = pairRecordRDD.mapValues(value -> 1)
				.reduceByKey((v1, v2) -> v1 + v2);

		// Join the two RDDs
		JavaPairRDD<String, Tuple2<Integer, Integer>> joinedRDD = freeSlotRDD.join(countSlotRDD);

		// joinedRDD.foreach(line -> System.out.println(line));

		// Compute the criticity for each (station and slot)
		JavaPairRDD<String, Double> criticityRDD = joinedRDD.mapValues(value -> new Double(100 * value._2 / value._1));

		// Filter the criticityRDD based on the threshold
		JavaPairRDD<String, Double> filteredCriticityRDD = criticityRDD.filter(value -> value._2 > threshold);

		// Select the most critical slot for each station
		JavaPairRDD<String, Tuple2<Double, String>> mostCriticalSlotRDD = filteredCriticityRDD.mapToPair(line -> {
			String[] fields = line._1.split("\t");
			return new Tuple2<>(fields[0], new Tuple2<>(line._2, fields[1]));
		}).reduceByKey((v1, v2) -> {
			if (v1._1 > v2._1)
				return v1;
			// If there are two or
			// more timeslots characterized by the highest criticality value for the same
			// station,
			// select only one of those timeslots. Specifically, select the one associated
			// with the
			// earliest hour. If also the hour is the same, consider the lexicographical
			// order of the
			// name of the week day
			else if (v1._1 == v2._1) {
				String[] fields1 = v1._2.split(" - ");
				String[] fields2 = v2._2.split(" - ");
				if (fields1[1].compareTo(fields2[1]) < 0)
					return v1;
				else if (fields1[1].compareTo(fields2[1]) > 0)
					return v2;
				else {
					if (fields1[0].compareTo(fields2[0]) < 0)
						return v1;
					else
						return v2;
				}

			}
			return v2;
		});

		// Now link each stationId to the associated coordinates
		// result is
		// * StationId
		// o Day of the week and hour of the critical timeslot
		// o Criticality value
		// o Coordinates of the station (longitude, latitude
		JavaPairRDD<String, Tuple2<Tuple2<Double, String>, String>> joinedRDD2 = mostCriticalSlotRDD
				.join(stationsPairRDD);

		JavaRDD<String> resultRDD = joinedRDD2.map(line -> {
			String[] fields = line._2._1._2.split(" - ");
			return line._1 + "\t" + fields[0] + "\t" + fields[1] + "\t" + line._2._1._1 + "\t" + line._2._2;
		});

		// Save the result in the output folder
		resultRDD.coalesce(1).saveAsTextFile(outputFolder);
	}
}
