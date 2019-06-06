package org.amit.dataSet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetFromMysql {
	
	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("Craete_Dataset_Myysql").master("local").getOrCreate();
		
        Dataset<Row> payload = sc
                .read()
                .format( "jdbc" )
                .option("driver", "com.mysql.jdbc.Driver")
                .option( "url", "jdbc:mysql://localhost:3306/uomdb?user=root&password=moglix" )
                .option( "dbtable", "uomdb.uom")
                .load();
        payload.printSchema();
        payload.createOrReplaceTempView("UOM");
        
        sc.sql("select * from UOM").show();

        
		
	}

}
