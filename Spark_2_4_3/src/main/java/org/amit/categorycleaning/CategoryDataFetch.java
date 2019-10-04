package org.amit.categorycleaning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class CategoryDataFetch {

	public static void main(String[] args) {
		//category_details, product_data
		SparkConf conf = new SparkConf().set("spark.driver.memory", "4g").setAppName("CategoryDataSetCassandra")
				.setMaster("local[10]");
		conf.set("spark.cassandra.connection.host", "54.169.1.174");
		conf.set("spark.cassandra.auth.username", "bagira");
		conf.set("spark.cassandra.auth.password", "Welc0M@2dju9gl@");

		SparkSession sc = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> cassdf = sc.read().format("org.apache.spark.sql.cassandra").option("table", "product_data")
				.option("keyspace", "products").load();

		cassdf.printSchema();

		cassdf.createOrReplaceTempView("QACassCat");

		Dataset<Row> savecassdf = sc.sql("select count(*) from QACassCat");

		//savecassdf.printSchema();

		savecassdf.show();
		//cassdf.repartition(1).write().format("JSON").save("/home/moglix/Desktop/Amit/Data_Spark/Prod_Cass_Cat");
		sc.close();
	}
}
