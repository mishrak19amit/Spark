package org.amit.Spark_2_3_1;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		SparkConf conf= new SparkConf().setAppName("Word_Count").setMaster("local");
		JavaSparkContext sc= new JavaSparkContext(conf);
		
		JavaRDD<String> lines=sc.textFile("/home/moglix/Desktop/Amit/PGitHub/Spark/data/Sample_Word_Count_Data.txt");
		
		System.out.println(lines.count());
		
		lines.foreach(x-> System.out.println(x));
		JavaRDD<String> words=lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		
		System.out.println(words.count());
		words.foreach(x-> System.out.println(x));
		
		JavaPairRDD<String, Integer> wordpair= words.mapToPair(x-> new Tuple2<String, Integer>(x,1));
		
		JavaPairRDD<String, Integer> wordcount=wordpair.reduceByKey((x,y)-> x+y);
		JavaPairRDD<Integer, String> swappedwordcount=wordcount.mapToPair(x-> x.swap()).sortByKey(false);
		wordcount=swappedwordcount.mapToPair(x-> x.swap());
		wordcount.foreach(x-> System.out.println(x._1()+" --> "+ x._2()));
		
		sc.close();
	}

}
