package org.amit.Spark_2_3_1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class JavaSparkSQLExample {

	public static void main(String[] args) {
		SparkSession sc= new SparkSession.Builder().appName("Basic_Spark_Session_Example").master("local").getOrCreate();
		
	Dataset<Row> peopledf	=sc.read().json("/home/moglix/Desktop/Amit/Spark2.3.1/spark/examples/src/main/resources/people.json");	
	peopledf.show();
	peopledf.printSchema();
	
	peopledf.select("age").show();
	peopledf.select(col("name"), col("age").plus(1)).show();;
	
	peopledf.filter(col("age").gt(18)).show();
	peopledf.groupBy(col("age")).count().show();
	System.out.println("Amit Mishra");
	
	peopledf.createOrReplaceTempView("people");
	Dataset<Row> df=sc.sql("select * from people");
	
	peopledf.createOrReplaceGlobalTempView("peoplegbl");
	
	sc.sql("select * from global_temp.peoplegbl").show();
	df.show();
	}
	
}
