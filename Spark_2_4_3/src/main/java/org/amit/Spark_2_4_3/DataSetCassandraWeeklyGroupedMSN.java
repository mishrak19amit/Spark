package org.amit.Spark_2_4_3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetCassandraWeeklyGroupedMSN {

	public static void main(String[] args) {
		SparkConf conf=new SparkConf();
		conf.set("spark.driver.memory", "1m");
		SparkSession sc = SparkSession.builder().config(conf).appName("Craete_Dataset_GroupedWeekly").master("local[*]").getOrCreate();
		
	Dataset<Row> productTempDF =sc.read().format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "13.234.108.103").option("spark.cassandra.auth.username", "bagira")
	.option("spark.cassandra.auth.password", "Welc0M@2dju9gl@").option("table", "product_data").option("keyspace", "products").load();

		//Dataset<Row> productTempDF=getDatasetFromFile(sc);
		productTempDF.printSchema();
		productTempDF.cache();
		productTempDF.createOrReplaceTempView("ProdProduct");
		
//		
		
		//Dataset<Row> updatedDF=sc.sql("select count(id_group)  from ProdProduct where  where updated_on >= '2019-06-24 00:00:00' AND updated_on <= '2019-06-30 23:59:59' and id_group is not null");
		//sc.sql("select count(id_group)  from ProdProduct where  where updated_on >= '2019-05-20 00:00:00' AND updated_on <= '2019-05-26 23:59:59' and id_group is not null").show();;
		//sc.sql("select count(id_group)  from ProdProduct where  where updated_on >= '2019-05-27 00:00:00' AND updated_on <= '2019-06-02 23:59:59' and id_group is not null").show();;
		//sc.sql("select count(id_group)  from ProdProduct where  where updated_on >= '2019-06-03 00:00:00' AND updated_on <= '2019-06-09 23:59:59' and id_group is not null").show();;
		//sc.sql("select count(id_group)  from ProdProduct where  where updated_on >= '2019-06-10 00:00:00' AND updated_on <= '2019-06-16 23:59:59' and id_group is not null").show();;
		//sc.sql("select count(id_group)  from ProdProduct where  where updated_on >= '2019-06-17 00:00:00' AND updated_on <= '2019-06-23 23:59:59' and id_group is not null").show();;
		//sc.sql("select count(id_group)  from ProdProduct where  where updated_on >= '2019-06-24 00:00:00' AND updated_on <= '2019-06-30 23:59:59' and id_group is not null").show();;
		Dataset<Row> updatedDF=sc.sql("select * from ProdProduct where id_product='MSN2VL3L7OYHPL'");
		//System.out.println(updatedDF.count());
		updatedDF.show();
		
//		updatedDF.repartition(1).write().format("JSON")
//		.save("/home/moglix/Desktop/Amit/Data_Spark/Prod_Cass_DeleteAttribute");


		sc.close();
	}
	
	public static Dataset<Row> getDatasetFromFile(SparkSession sc){
		return sc.read().json("/home/moglix/Desktop/Task/Product_Dump_Prod_5July_Morning.json");
	}

}
