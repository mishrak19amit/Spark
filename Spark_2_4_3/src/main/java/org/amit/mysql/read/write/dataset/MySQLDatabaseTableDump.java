package org.amit.mysql.read.write.dataset;

import org.amit.orderanalysis.Util.OrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MySQLDatabaseTableDump {

	public static void main(String[] args) {
		//order_details, order_items
		SparkSession sc = SparkSession.builder().appName("CassProduct_Myysql").master("local[*]").getOrCreate();

		Dataset<Row> payload = sc.read().format("jdbc").option("driver", "com.mysql.jdbc.Driver")
				.option("url", "jdbc:mysql://52.76.148.206:3306/new_online_platform?user=amitmishra&password=Am1t@321")
				.option("dbtable", "new_online_platform.order_items").load();
		payload.printSchema();
		payload.createOrReplaceTempView("OnlineTable");

		Dataset<Row> updatedPayload = sc.sql("select * from OnlineTable");

		updatedPayload.show();

		OrderUtil.saveDSAsJson(updatedPayload, "order_items");
	}

}
