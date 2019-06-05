package org.amit.Spark_2_3_1;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Word_Count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// /var/log/moglix/platform/dataload
		// /home/moglix/Desktop/Amit/PGitHub/Spark/data/Sample_Word_Count_Data.txt
		JavaRDD<String> lines = sc.textFile("/var/log/moglix/platform/dataload/*");

		JavaRDD<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordpair = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1));

		JavaPairRDD<String, Integer> wordcount = wordpair.reduceByKey((x, y) -> x + y);
		JavaPairRDD<Integer, String> swappedwordcount = wordcount.mapToPair(x -> x.swap()).sortByKey(false);

		wordcount = swappedwordcount.mapToPair(x -> x.swap());
		JavaRDD<String> wordcountInfo = words.filter(x -> x.contains("INFO"));
		JavaRDD<String> wordcountError = words.filter(x -> x.contains("ERROR"));
		System.out.println("Total number of Info :" + wordcountInfo.count());
		System.out.println("Total number of Error :" + wordcountError.count());
		wordcount = wordcount.filter(x -> 1000 < x._2());
		wordcount.foreach(x -> System.out.println(x._1() + " --> " + x._2()));
		System.out.println("Amit Mishra");
		sc.close();
	}

}
