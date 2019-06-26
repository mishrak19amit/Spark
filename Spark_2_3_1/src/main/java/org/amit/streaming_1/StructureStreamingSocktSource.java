package org.amit.streaming_1;

import java.util.Arrays;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StructureStreamingSocktSource {
	private static final Logger logger = LogManager.getLogger(StructureStreamingSocktSource.class);

	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("Revision_StructuredStreamin").master("local[*]")
				.getOrCreate();
		Dataset<Row> lines = sc.readStream().format("socket").option("host", "127.0.0.1").option("port", "9999").load();

		Dataset<String> words = lines.as(Encoders.STRING()).flatMap(x -> {
			return Arrays.asList(x.split(" ")).iterator();
		}, Encoders.STRING());

		Dataset<Row> wordcounts = words.groupBy("value").count();

		StreamingQuery squery = wordcounts.writeStream().outputMode("complete").format("console").start();

		try {
			squery.awaitTermination();
		} catch (StreamingQueryException e) {
			logger.error(e);
		}
	}

}
