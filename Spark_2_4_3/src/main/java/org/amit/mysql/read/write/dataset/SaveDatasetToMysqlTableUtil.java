package org.amit.mysql.read.write.dataset;

import java.util.Properties;

import org.amit.orderanalysis.Util.OrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SaveDatasetToMysqlTableUtil {

	public static void saveDatasetToMysqlTable(Dataset<Row> dataset, String tableName) {
		Properties prop = new Properties();
		prop.setProperty("driver", "com.mysql.jdbc.Driver");
		prop.setProperty("user", "root");
		prop.setProperty("password", "moglix");

		String url = "jdbc:mysql://localhost:3306/searchAnalysis?useUnicode=true&character_set_server=utf8mb4";

		dataset.write().mode(SaveMode.Append).jdbc(url, tableName, prop);

	}
	
	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("CassProduct_Myysql").master("local[*]").getOrCreate();

		Dataset<Row> orderDS=OrderUtil.getDatasetFromJSONFile("/home/moglix/orderDataAnalysis/cassMysqlDataset/order_items.json", sc);
		
		saveDatasetToMysqlTable(orderDS, "order_items");
	}

}
