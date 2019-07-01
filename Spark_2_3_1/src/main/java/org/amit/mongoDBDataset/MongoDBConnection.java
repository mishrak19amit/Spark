package org.amit.mongoDBDataset;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class MongoDBConnection {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[5]").appName("MongoSparkConnectorIntro")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection").getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		jsc.close();
	}

}
