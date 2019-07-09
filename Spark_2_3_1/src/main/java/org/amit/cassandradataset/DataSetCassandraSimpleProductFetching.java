package org.amit.cassandradataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetCassandraSimpleProductFetching {

	public static void main(String[] args) {
		
		SparkSession sc = SparkSession.builder().appName("Craete_Dataset_Myysql").master("local[*]").getOrCreate();
		
		Dataset<Row> productTempDF =sc.read().format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "54.169.17.194").option("spark.cassandra.auth.username", "bagira")
		.option("spark.cassandra.auth.password", "Welc0M@2dju9gl@").option("table", "product_data_temp").option("keyspace", "products").load();

		productTempDF.printSchema();
		productTempDF.createOrReplaceTempView("ProdProduct");
		
		Dataset<Row> fileDF=getDatasetFromFile(sc);
		fileDF.createOrReplaceTempView("FileDF");
		
		Dataset<Row> updatedDF=sc.sql("select ProdProduct.id_product, ProdProduct.created_on, FileDF.updated_on from ProdProduct, FileDF where ProdProduct.id_product=FileDF.id_product");
	
		updatedDF.show();
		
		Dataset<String> csql=updatedDF.map(x->{
			return x.getString(0);
		}, Encoders.STRING());
		
		csql.show();
		
		
		//System.out.println(updatedDF.count());
		
		updatedDF.repartition(1).write().format("CSV").save("/home/moglix/Desktop/Amit/Data_Spark/Prod_Cass_Product");
		
		sc.close();
	}
	
	public static Dataset<Row> getDatasetFromFile(SparkSession sc){
		return sc.read().json("/home/moglix/Desktop/Task/dataUpdateProduct/Product_Prod.json");
	}
}
