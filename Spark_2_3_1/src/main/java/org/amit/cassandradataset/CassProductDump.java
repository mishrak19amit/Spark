package org.amit.cassandradataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CassProductDump {

	public static void main(String[] args) {
		String qaIP="13.234.108.103";
		String prodIP="18.139.106.59";
		String productData="product_data";
		String productDataRaw="product_data_raw";
		String filePath="/home/moglix/Desktop/Amit/Data_Spark/Cass_Product_Dump";
		
		CassProductDump obj= new CassProductDump();
		SparkSession sc = SparkSession.builder().appName("Product_Dump").master("local[*]").getOrCreate();
		
		obj.getProductJsonDump(sc, prodIP, productData, filePath+"_Prod");
		//obj.getProductJsonDump(sc, qaIP, productDataRaw, filePath+"_QA");

		sc.close();
	}

	public void getProductJsonDump(SparkSession sc, String cassIP, String tableName, String folderPath) {
		
		Dataset<Row> productTempDF =sc.read().format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", cassIP).option("spark.cassandra.auth.username", "bagira")
		.option("spark.cassandra.auth.password", "Welc0M@2dju9gl@").option("table", tableName).option("keyspace", "products").load();

		productTempDF.printSchema();
		productTempDF.createOrReplaceTempView("ProductView");
		
		Dataset<Row> updatedDF=sc.sql("select * from ProductView");
	
		updatedDF.show();
		
		//System.out.println(updatedDF.count());
		
		updatedDF.repartition(1).write().format("JSON").save(folderPath);
		
		
	}

}
