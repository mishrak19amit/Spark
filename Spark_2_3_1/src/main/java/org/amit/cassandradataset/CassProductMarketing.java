package org.amit.cassandradataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CassProductMarketing {

	public static void main(String[] args) {
		// 13.234.108.103
		// 18.139.106.59
		SparkSession sc = SparkSession.builder().appName("Product_Marketing_Custom").master("local[1]").getOrCreate();
		
		Dataset<Row> productTempDF =sc.read().format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "18.139.106.59").option("spark.cassandra.auth.username", "bagira")
		.option("spark.cassandra.auth.password", "Welc0M@2dju9gl@").option("table", "product_data").option("keyspace", "products").load();

		productTempDF.printSchema();
		productTempDF.createOrReplaceTempView("QAProduct");
		
		
		Dataset<Row> updatedDF=sc.sql("select QAProduct.id_product, QAProduct.analytical_params['custom'] as CustomVal from QAProduct where QAProduct.analytical_params['custom']=1");
	
		updatedDF.show();
		
		System.out.println(updatedDF.count());
		
//		Dataset<String> csql=updatedDF.map(x->{
//			return x.getString(0);
//		}, Encoders.STRING());
//		
//		csql.show();
		
		
		//System.out.println(updatedDF.count());
		
		//updatedDF.repartition(1).write().format("CSV").save("/home/moglix/Desktop/Amit/Data_Spark/Cass_Product_Marketing");
		
		sc.close();
	}
	
	public static Dataset<Row> getDatasetFromFile(SparkSession sc){
		return sc.read().json("/home/moglix/Desktop/Task/dataUpdateProduct/Product_Prod.json");
	}
}
