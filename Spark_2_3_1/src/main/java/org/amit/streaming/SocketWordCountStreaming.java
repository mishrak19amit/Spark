package org.amit.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SocketWordCountStreaming {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("NetworkWordCountStreaming").setMaster("local[3]");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
		JavaReceiverInputDStream<String> socketStream=jsc.socketTextStream("localhost", 9999);
		JavaDStream<String> dstreamWord=socketStream.flatMap(x-> Arrays.asList(x.split(" ")).iterator());
		JavaPairDStream<String, Integer> pairDstreamWord=dstreamWord.mapToPair(x-> new Tuple2<>(x.toLowerCase(),1));
		JavaPairDStream<String, Integer> countedPairDstreamWord=pairDstreamWord.reduceByKey((a,b)-> a+b);
		countedPairDstreamWord.print();
		
		jsc.start();
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		finally {
			jsc.close();
		}
	}
}
