package org.amit.sia.chapter3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class LoadJson {

	public static void main(String[] args) {
		SparkSession spark= SparkSession.builder().appName("JsonLoadDemo").master("local[10]").getOrCreate();
		Dataset<Row> jsondf=spark.read().json("/home/moglix/Desktop/Amit/Data_Spark/sia/github-archive/2015-03-01-0.json");
		jsondf.show(2);
		Dataset<Row> filteredjsondf=jsondf.filter("type = 'PushEvent'");
		//filteredjsondf.printSchema();
		System.out.println(filteredjsondf.count());
		System.out.println(jsondf.count());
		Dataset<Row> groupeddf=filteredjsondf.groupBy("actor.login").count();
		groupeddf.printSchema();
		Dataset<Row> ordereddf=groupeddf.sort(desc("count"));
		ordereddf.show();
	}
	
}
