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

		try {

			JavaRDD<String> lines = sc.textFile(
					"/home/moglix/Desktop/Amit/Data_Spark/first-edition-master/ch04/ch04_data_transactions.txt");
			JavaPairRDD<Integer, String> pairrdd = lines.mapToPair(x -> {
				int key = Integer.parseInt(x.split("#")[2]);
				return new Tuple2<>(key, x);

			});

			// Caching RDD 
			pairrdd.cache();
			
			System.out.println("Initial RDD count: "+pairrdd.count());
			
			// [Key-53] Offering bear doll, who has made most transaction //
			pairrdd = OfferingBabyDoll(pairrdd);

			// [key-17] Giving 5% discount which have bought more than one Barbie doll //
			pairrdd = Offering5PerDiscountForMoreBarbie(pairrdd);

			// Keys[]85,82,10,16,47 Adding toothbrush who have bought more than 5 dictionary //
			pairrdd = OfferingToothBrushForMoreThanFiveDictionary(pairrdd);

			// Key[76] Pair of payjama who spent most money //
			pairrdd = OfferingPairPayjamaForMostTransaction(pairrdd);

			System.out.println("Final RDD count: " + pairrdd.count());

		} catch (Exception e) {
			System.out.println(e);
		} finally {
			sc.close();
		}
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

	public static JavaPairRDD<Integer, String> addingProductToPairRDD(JavaPairRDD<Integer, String> pairrdd,
			int mostspentmoneykey, String payjama) {

		pairrdd = pairrdd.groupByKey().flatMapValues(x -> {
			int keyofmosttrans = Integer.parseInt(x.iterator().next().split("#")[2]);
			if (keyofmosttrans == mostspentmoneykey) {
				List<String> list = new ArrayList<>();
				x.forEach(xx -> list.add(xx));
				list.add(payjama);
				return list;
			} else {
				return x;
			}

		});

		return pairrdd;
	}

	public static JavaPairRDD<Integer, String> OfferingBabyDoll(JavaPairRDD<Integer, String> pairrdd) {

		Map<Integer, Long> countedbyKeyMap = pairrdd.countByKey();
		int key = getMostTransaction(countedbyKeyMap);
		System.out.println(key + " has made the most transactions " + countedbyKeyMap.get(key));
		String beardoll = "2015-03-30#11:59 PM#" + key + "#4#1#0.00";
		pairrdd = addingProductToPairRDD(pairrdd, key, beardoll);
		return pairrdd;

	}

	public static JavaPairRDD<Integer, String> Offering5PerDiscountForMoreBarbie(JavaPairRDD<Integer, String> pairrdd) {

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

		return pairrdd;
	}

	public static JavaPairRDD<Integer, String> OfferingToothBrushForMoreThanFiveDictionary(
			JavaPairRDD<Integer, String> pairrdd) {

		pairrdd = pairrdd.groupByKey().flatMapValues(x -> {

			int finaldickey = -1;
			List<String> list = new ArrayList<>();
			x.forEach(xx -> list.add(xx));
			for (String xx : list) {
				String[] values = xx.split("#");
				int dickey = Integer.parseInt(values[2]);
				int itemID = Integer.parseInt(values[3]);
				Double quantity = Double.parseDouble(values[4]);
				if (itemID == 81 && 5 < quantity) {
					finaldickey = dickey;
					break;
				}

			}

			if (finaldickey != -1) {
				String toothbrush = "2015-03-30#11:59 PM#" + finaldickey + "#70#1#0.00";
				System.out.println(finaldickey);
				list.add(toothbrush);
				return list;
			} else {
				return x;
			}

		});

		return pairrdd;
	}

	public static JavaPairRDD<Integer, String> OfferingPairPayjamaForMostTransaction(
			JavaPairRDD<Integer, String> pairrdd) {
		JavaPairRDD<Integer, Double> spentMoney = pairrdd.mapToPair(x -> {
			double value = Double.parseDouble(x._2().split("#")[5]);
			return new Tuple2<Integer, Double>(x._1(), value);
		});

		JavaPairRDD<Integer, Double> spentMoneycount = spentMoney.reduceByKey((a, b) -> a + b);

		JavaPairRDD<Double, Integer> spentMoneysorted = spentMoneycount.mapToPair(x -> {
			return new Tuple2<Double, Integer>(x.swap()._1(), x.swap()._2());
		}).sortByKey(false);

		int mostspentmoneykey = spentMoneysorted.first()._2();
		System.out.println("Most money spent by : " + spentMoneysorted.first()._2() + " and Spent money is: "
				+ spentMoneysorted.first()._1());

		String payjama = "2015-03-30#11:59 PM#" + spentMoneysorted.first()._2() + "#101#1#0.00";
		pairrdd = addingProductToPairRDD(pairrdd, mostspentmoneykey, payjama);
		pairrdd.lookup(mostspentmoneykey).forEach(x -> System.out.println(x));

		return pairrdd;
	}
}
