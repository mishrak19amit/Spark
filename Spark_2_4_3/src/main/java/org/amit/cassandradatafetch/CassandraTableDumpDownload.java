package org.amit.cassandradatafetch;

import org.amit.orderanalysis.Util.CassandraDatafetchUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CassandraTableDumpDownload {

	public static void main(String[] args) {
		// category_details, product_data, attribute_details, brand_details
		// 18.139.106.59, 13.234.108.103

		SparkSession sc = getCassSparkSession();

		Dataset<Row> cassProductDF = getCassTableDataset(sc, "product_data");
		CassandraDatafetchUtil.saveDSAsCSV(cassProductDF, "product_data");

		sc.close();
	}

	public static Dataset<Row> getCassTableDataset(SparkSession sc, String tableName) {

		Dataset<Row> cassdf = sc.read().format("org.apache.spark.sql.cassandra").option("table", tableName)
				.option("keyspace", "products").load();

		boolean isFilter=true;
		if(isFilter) {
			Dataset<Row> filteredCassdf=doFiltering(sc, cassdf);
			return filteredCassdf;
		}
		else {
			return cassdf;
		}
	}

	private static Dataset<Row> doFiltering(SparkSession sc, Dataset<Row> cassdf) {
		cassdf.createOrReplaceTempView("ProdCassCat");

		Dataset<Row> filteredCassdf = sc.sql("SELECT id_product from ProdCassCat");
		return filteredCassdf;
	}

	public static SparkSession getCassSparkSession() {

		SparkConf conf = new SparkConf().set("spark.driver.memory", "4g").setAppName("ProductDataSetCassandra")
				.setMaster("local[10]");
		conf.set("spark.cassandra.connection.host", "18.139.106.59");
		conf.set("spark.cassandra.auth.username", "bagira");
		conf.set("spark.cassandra.auth.password", "Welc0M@2dju9gl@");

		SparkSession sc = SparkSession.builder().config(conf).getOrCreate();
		return sc;
	}
}
