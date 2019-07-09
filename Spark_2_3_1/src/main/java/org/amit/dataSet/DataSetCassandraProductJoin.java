package org.amit.dataSet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class DataSetCassandraProductJoin {

	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("Dataset_Cassandra_Join").master("local[*]").getOrCreate();

		Encoder<ProductDateUpdate> employeeEncoder = Encoders.bean(ProductDateUpdate.class);
		String path = "/home/moglix/Desktop/Task/dataUpdateProduct/Product_With_Updated_Date.json";
		Dataset<ProductDateUpdate> ds = sc.read().json(path).as(employeeEncoder);
		ds.show();

		ds.printSchema();

		Dataset<String> csql = ds.map(x -> {
			return "UPDATE products.product_data_Temp  set created_on='" + x.getCreated_on() + "' where id_product='"
					+ x.getId_product() + "';";
		}, Encoders.STRING());

		csql.show();

		csql.repartition(1).write().format("TEXT").save("/home/moglix/Desktop/Amit/Data_Spark/Prod_Cass_Product");

		sc.close();
	}
}
