package org.amit.Spark_2_4_3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetCassandraProduct {

	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("Dataset_Cassandra").master("local[*]").getOrCreate();
		Dataset<Row> cassdf = sc.read()
				.json("/home/moglix/Desktop/Task/Category/Prod_Product_With_Item_in_Pack_ID.json");

		cassdf.createOrReplaceTempView("QACass");

		Dataset<Row> savecassdf = sc.sql(
				"select id_product, attribute_list['bb85c6a9-c33b-4e1c-a108-60d340e765d4'][0] as Item_In_Pack_ID_Value, items_in_pack, created_on, updated_on from QACass");
		savecassdf.show();
		savecassdf.repartition(1).write().format("csv")
				.save("/home/moglix/Desktop/Amit/Data_Spark/Prod_Cass_Item_In_Pack_Csv");
		sc.close();
	}
}
