package org.amit.dataSet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatasetCsvFile {
	
	public static void main(String[] args) {
		SparkSession sc= SparkSession.builder().appName("Dataset_CSV").master("local").getOrCreate();
		Dataset<Row> csvdf=sc.read().format("csv").option("header", true).load("/home/moglix/Desktop/Amit/PGitHub/Spark/data/UIDAI-ENR-DETAIL-20170308.csv");
		
		csvdf.printSchema();
		
		csvdf.createOrReplaceTempView("Aadhar");
		
		long totalregistrar=sc.sql("select Distinct Registrar from Aadhar ").count();
		
		System.out.println(totalregistrar);
		
		sc.sql("SELECT State, SUM(`Aadhaar generated`) as count FROM Aadhar GROUP BY state ORDER BY count DESC").show();
	}

}
