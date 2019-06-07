package org.amit.structurestreaming;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class WordCountOnStream {

	public static void main(String[] args) {
		// nc -lk 9999
		SparkSession sc = SparkSession.builder().appName("StructureStreaming").master("local").getOrCreate();

		Dataset<Row> lines = sc.readStream().format("socket").option("host", "localhost").option("port", 6379).load();

		Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
				(FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

		Dataset<Row> wordCounts = words.groupBy("value").count();

		//StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

		StreamingQuery kafkaquery = wordCounts.writeStream().format("kafka").outputMode("complete")
				.option("kafka.bootstrap.servers", "localhost:9092").option("topic", "structureStreaming").option("checkpointLocation", "/home/moglix/StructureStreaming").start();
		try {
			//query.awaitTermination();
			kafkaquery.awaitTermination();
		} catch (StreamingQueryException e) {
			System.out.println(e);
		}

	}

}
