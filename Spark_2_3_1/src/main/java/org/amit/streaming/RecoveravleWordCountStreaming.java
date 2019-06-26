package org.amit.streaming;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import shade.com.datastax.spark.connector.google.common.io.Files;

class JavaWordBlacklist {
	private static volatile Broadcast<List<String>> instance = null;

	public static Broadcast<List<String>> getInstance(JavaSparkContext sc) {
		if (instance == null) {
			synchronized (JavaWordBlacklist.class) {
				instance = sc.broadcast(Arrays.asList("a", "b", "c"));
			}

		}

		return instance;
	}

}

class JavaBlackListDemo {

	private static volatile Broadcast<List<String>> instance = null;

	public Broadcast<List<String>> getBroadCast(JavaSparkContext sc) {
		if (null == instance) {

			synchronized (JavaBlackListDemo.class) {

				return instance = sc.broadcast(Arrays.asList("a", "b", "c"));
			}

		} else {
			return instance;
		}
	}
}

class JavaDroppedWordCounterDemo{
	private static volatile LongAccumulator instance=null;
	
	public LongAccumulator getAccumulator(JavaSparkContext sc) {
		if(null==instance) {
			
			synchronized (JavaDroppedWordCounterDemo.class) {
				return instance=sc.sc().longAccumulator("WordBlackListCounter");
			}
		}
		else {
			return instance;
		}
	}
}

class JavaDroppedWordsCounter {
	private static volatile LongAccumulator instance = null;

	public static LongAccumulator getInstance(JavaSparkContext sc) {

		if (null == instance) {
			synchronized (JavaDroppedWordsCounter.class) {

				instance = sc.sc().longAccumulator("WordsInBlacklistCounter");

			}
		}

		return instance;
	}
}

public class RecoveravleWordCountStreaming {

	public static JavaStreamingContext createContext(String hostName, int port, String checkpointDirectory,
			String outputPath) {
		SparkConf conf = new SparkConf().setAppName("RecoverableWordCountStreaming").setMaster("local[3]");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
		System.out.println("Creating Spark Context...");
		File outputFile = new File(outputPath);
		if (outputFile.exists()) {
			outputFile.delete();
		}
		jsc.checkpoint(checkpointDirectory);
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream(hostName, port);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(x -> new Tuple2<>(x, 1))
				.reduceByKey((a, b) -> a + b);
		wordCounts.foreachRDD((rdd, time) -> {
			Broadcast<List<String>> blacklist = JavaWordBlacklist.getInstance(new JavaSparkContext(rdd.context()));
			LongAccumulator droppedWordCounter = JavaDroppedWordsCounter
					.getInstance(new JavaSparkContext(rdd.context()));

			String counts = rdd.filter(wordcount -> {
				if (blacklist.value().contains(wordcount._1())) {
					droppedWordCounter.add(wordcount._2());
					return false;
				} else
					return true;
			}).collect().toString();
			String output = "Counts at time " + time + " " + counts;
			System.out.println(output);
			System.out.println("Dropped " + droppedWordCounter.value() + " word(s) totally");
			System.out.println("Appending to " + outputFile.getAbsolutePath());
			Files.append(output + "\n", outputFile, Charset.defaultCharset());

		});

		return jsc;

	}

	public static void main(String[] args) {
		String hostName = "localhost";
		int port = 9999;
		String checkpointDirectory = "/home/moglix/Desktop/Amit/Data_Spark/checkPointDir";
		String outputPath = "/home/moglix/Desktop/Amit/Data_Spark/Streaming";

		Function0<JavaStreamingContext> createContextFunc = () -> createContext(hostName, port, checkpointDirectory,
				outputPath);
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
