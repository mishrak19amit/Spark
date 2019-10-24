package org.amit.cassandradataset;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class QABuyerMysql {
	
	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("CassProduct_Myysql").master("local[*]").getOrCreate();
		
        Dataset<Row> payload = sc
                .read()
                .format( "jdbc" )
                .option("driver", "com.mysql.jdbc.Driver")
                .option( "url", "jdbc:mysql://localhost:3306/uomdb?user=root&password=moglix" )
                .option( "dbtable", "uomdb.uom")
                .load();
        payload.printSchema();
        payload.createOrReplaceTempView("UOM");
        
        Dataset<Row> updatedPayload=sc.sql("select id, uom from UOM");
  	
	}

}
