package org.amit.sia.chapter4;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TranscationAnalytics {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Transcation_Analysis").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc
				.textFile("/home/moglix/Desktop/Amit/Data_Spark/first-edition-master/ch04/ch04_data_transactions.txt");
		JavaPairRDD<Integer, String> pairrdd = lines.mapToPair(x -> {
			int key = Integer.parseInt(x.split("#")[2]);
			return new Tuple2<>(key, x);

		});
		Map<Integer, Long> countedbyKeyMap = pairrdd.countByKey();
		int key = getMostTransaction(countedbyKeyMap);

		System.out.println(key + " has made the most transactions " + countedbyKeyMap.get(key));

		pairrdd.lookup(key).forEach(x -> System.out.println(x));
		System.out.println(pairrdd.keys().distinct().count());
		String newvalues = "2015-03-30#11:59 PM#53#4#1#0.00";
		Tuple2<Integer, String> newvalue = new Tuple2<Integer, String>(key, newvalues);
		// pairrdd.foreach(x -> System.out.println(x));

		System.out.println(pairrdd.count());
		JavaPairRDD<Integer, Iterable<String>> grouprdd = pairrdd.groupByKey();
		pairrdd = grouprdd.flatMapValues(x -> {
			int keyofmosttrans = Integer.parseInt(x.iterator().next().split("#")[2]);
			if (keyofmosttrans == key) {
				List<String> list = new ArrayList<>();
				x.forEach(xx -> list.add(xx));
				list.add(newvalues);
				return list;
			} else {
				return x;
			}

		});
		System.out.println(pairrdd.count());

		pairrdd.lookup(key).forEach(x -> System.out.println(x));

		System.out.println("Amit");

		pairrdd = pairrdd.mapValues(x -> {

			String[] values = x.split("#");
			int itemID = Integer.parseInt(values[3]);
			Double quantity = Double.parseDouble(values[4]);
			Double price = Double.parseDouble(values[5]);

			if (itemID == 25 && 1 < quantity) {
				price = price * 0.95;
			}
			values[5] = price.toString();
			String finalvalue = "";
			for (int i = 0; i < values.length - 1; i++) {
				finalvalue += values[i] + "#";
			}
			finalvalue += values[values.length - 1];

			return finalvalue;
		});

		pairrdd.lookup(17).forEach(x -> System.out.println(x));

		sc.close();
	}

	public static int getMostTransaction(Map<Integer, Long> valuMap) {
		int max = 0;
		int valueKey = -1;
		for (int key : valuMap.keySet()) {
			int value = valuMap.get(key).intValue();
			if (max < value) {
				max = value;
				valueKey = key;
			}
		}

		return valueKey;
	}
}
