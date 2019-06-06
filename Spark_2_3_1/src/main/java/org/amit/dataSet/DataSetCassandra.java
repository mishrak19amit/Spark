package org.amit.dataSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataSetCassandra {

	public static void main(String[] args) {
		//SparkSession sc= SparkSession.builder().appName("Dataset Cassandra").master("local").getOrCreate();
		SparkConf conf= new SparkConf().setAppName("Cassandra_Connection").setMaster("local");
		conf.set("spark.cassandra.connection.host", "54.169.17.194");
        conf.set("spark.cassandra.auth.username", "bagira");          
        conf.set("spark.cassandra.auth.password", "Welc0M@2dju9gl@");
        
        JavaSparkContext jsc=new JavaSparkContext(conf);
        
        System.out.println(jsc.appName());
        
        SQLContext sqlcontext= new SQLContext(jsc);
       Dataset<Row>  cassdf=sqlcontext.read().format("org.apache.spark.sql.cassandra").option("table", "product_data_raw").option("keyspace", "products").load();
        
       cassdf.printSchema();
       
       cassdf.createOrReplaceTempView("QACass");
       
       Dataset<Row>  savecassdf= sqlcontext.sql("select * from QACass");
        
       savecassdf.write().format("json").save("/home/moglix/Desktop/Amit/PGitHub/Spark/data/QA_Cass_Json");
        jsc.close();
	}
}
