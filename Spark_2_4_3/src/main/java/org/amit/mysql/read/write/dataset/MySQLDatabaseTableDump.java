package org.amit.mysql.read.write.dataset;

import org.amit.orderanalysis.Util.OrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MySQLDatabaseTableDump {

	public static void main(String[] args) {
		// order_details, order_items

		SparkSession sc = getSparkSession();
		Dataset<Row> payload = getMysqlTableDataset(sc, "order_details");

		payload.show();
		//OrderUtil.saveDSAsJson(payload, "order_details");
	}

	public static Dataset<Row> getMysqlTableDataset(SparkSession sc, String tableName) {

		Dataset<Row> payload = sc.read().format("jdbc").option("driver", "com.mysql.jdbc.Driver")
				.option("url", "jdbc:mysql://52.76.148.206:3306/new_online_platform?user=amitmishra&password=Am1t@321")
				.option("dbtable", "new_online_platform.order_items").load();

		return payload;
	}

	public static SparkSession getSparkSession()

	{
		SparkSession sc = SparkSession.builder().appName("CassProduct_Myysql").master("local[*]").getOrCreate();

		return sc;
	}

}
