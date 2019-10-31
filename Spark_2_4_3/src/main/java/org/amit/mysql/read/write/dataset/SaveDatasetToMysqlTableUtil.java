package org.amit.mysql.read.write.dataset;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class SaveDatasetToMysqlTableUtil {

	public static void saveDatasetToMysqlTable(Dataset<Row> dataset, String tableName) {
		Properties prop = new Properties();
		prop.setProperty("driver", "com.mysql.jdbc.Driver");
		prop.setProperty("user", "root");
		prop.setProperty("password", "moglix");

		String url = "jdbc:mysql://localhost:3306/orderAnalysis?useUnicode=true&character_set_server=utf8mb4";

		dataset.write().mode(SaveMode.Append).jdbc(url, tableName, prop);

	}

}
