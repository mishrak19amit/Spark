package org.amit.structurestreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class StreamFromCSV {
	
	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("StructureStreamingOnCSV").master("local").getOrCreate();
		StructType userschema= new StructType().add("name", "string").add("age", "integer");
		Dataset<Row> csvDF=sc.readStream().option("sep", ",").schema(userschema).csv("/home/moglix/Desktop/Amit/PGitHub/Spark/data/StructureStreaming/*");
		csvDF.printSchema();
		Dataset<Row> csvCounts = csvDF.groupBy("name").count();
		StreamingQuery query= csvCounts.writeStream().outputMode("complete").format("console").start();
		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
		
	}

}
