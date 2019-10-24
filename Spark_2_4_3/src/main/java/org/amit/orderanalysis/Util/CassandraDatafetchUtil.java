package org.amit.orderanalysis.Util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CassandraDatafetchUtil {

	public static Dataset<Row> getDatasetFromCSVFile(String fileName, SparkSession sc) {

		Dataset<Row> ds = sc.read().csv(fileName);

		return ds;

	}

	public static Dataset<Row> getDatasetFromJSONFile(String fileName, SparkSession sc) {

		Dataset<Row> ds = sc.read().json(fileName);

		return ds;

	}

	public static void printColumns(Dataset<Row> ds) {

		String[] cols = ds.columns();

		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (i = 0; i < cols.length - 1; i++) {
			sb.append(cols[i]).append(",");
		}
		sb.append(cols[i]);

		System.out.println("Columns list of Dataset: " + sb.toString());

	}
	
	public static void saveDSAsJson(Dataset<Row> dataset, String fileName) {
		dataset.repartition(1).write().format("JSON").save("/home/moglix/orderDataAnalysis/ProdCassandraDataset/"+fileName);
	}

}
