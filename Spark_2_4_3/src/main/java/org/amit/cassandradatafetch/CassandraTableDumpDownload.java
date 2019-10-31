package org.amit.cassandradatafetch;

import org.amit.orderanalysis.Util.CassandraDatafetchUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CassandraTableDumpDownload {

	public static void main(String[] args) {
		//category_details, product_data, attribute_details, brand_details
		//18.139.106.59, 13.234.108.103
		SparkConf conf = new SparkConf().set("spark.driver.memory", "4g").setAppName("ProductDataSetCassandra")
				.setMaster("local[10]");
		conf.set("spark.cassandra.connection.host", "18.139.106.59");
		conf.set("spark.cassandra.auth.username", "bagira");
		conf.set("spark.cassandra.auth.password", "Welc0M@2dju9gl@");

		SparkSession sc = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> cassdf = sc.read().format("org.apache.spark.sql.cassandra").option("table", "brand_details")
				.option("keyspace", "products").load();

		cassdf.printSchema();

		cassdf.createOrReplaceTempView("ProdCassProduct");

		Dataset<Row> savecassdf = sc.sql("select * from ProdCassProduct");

		CassandraDatafetchUtil.saveDSAsJson(savecassdf, "brand_details");
		
		sc.close();
	}
}
