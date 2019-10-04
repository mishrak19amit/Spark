package org.amit.cassandradataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BrandsFetchIDName {

	public static void main(String[] args) {
		
		SparkSession sc = SparkSession.builder().appName("Brands_Fetch").master("local[*]").getOrCreate();
		
		Dataset<Row> brandDF =sc.read().format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "54.169.1.174").option("spark.cassandra.auth.username", "bagira")
		.option("spark.cassandra.auth.password", "Welc0M@2dju9gl@").option("table", "brand_details").option("keyspace", "products").load();

		//brandDF.printSchema();
		brandDF.createOrReplaceTempView("BrandView");
		
		Dataset<Row> brandFileDF=getDatasetFromFile(sc);
		brandFileDF.printSchema();
		brandFileDF.createOrReplaceTempView("BrandFileDF");
		
		//Dataset<Row> updatedDF=sc.sql("select BrandView.brand_name, BrandView.id_brand, BrandFileDF._c0, BrandFileDF._c1 from BrandView, BrandFileDF where BrandView.brand_name==BrandFileDF._c0");
		Dataset<Row> updatedDF=sc.sql("select BrandFileDF._c0 from BrandView, BrandFileDF where BrandView.id_brand==BrandFileDF._c1");
	
//		Dataset<Row> fileOnlyName=sc.sql("select _c0 from BrandFileDF");
//		fileOnlyName.show();
//		System.out.println(fileOnlyName.count());
//		updatedDF.createOrReplaceTempView("BrandOnlyName");
//		Dataset<Row> brandOnlyName=sc.sql("select brand_name from BrandOnlyName");
//		brandOnlyName.show();
//		System.out.println(brandOnlyName.count());
//		
//		brandOnlyName.repartition(1).write().format("CSV").save("/home/moglix/Desktop/Amit/Data_Spark/brandOnlyName");
//		fileOnlyName.repartition(1).write().format("CSV").save("/home/moglix/Desktop/Amit/Data_Spark/fileOnlyName");
//		
//		Dataset<Row> distinctName=fileOnlyName.intersect(brandOnlyName);
//		distinctName.show();
//		distinctName.repartition(1).write().format("CSV").save("/home/moglix/Desktop/Amit/Data_Spark/Different_ID_Brand");
//		
//		updatedDF.show();
//		
//		Dataset<String> csql=updatedDF.map(x->{
//			return x.getString(0);
//		}, Encoders.STRING());
//		
//		csql.show();
//		
//		
//		//System.out.println(updatedDF.count());
//		
//		Dataset<Row> updatedDF=sc.sql("select id_brand, brand_name from BrandView");
		updatedDF.repartition(1).write().format("CSV").save("/home/moglix/Desktop/Amit/Data_Spark/Prod_Cass_Brand");	
		sc.close();
	}
	
	public static Dataset<Row> getDatasetFromFile(SparkSession sc){
		return sc.read().csv("/home/moglix/Desktop/Task/Brands/BrandName225.csv");
	}
}
