package org.amit.dataSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataSetCassandra {

	public static void main(String[] args) {
		//SparkSession sc= SparkSession.builder().appName("Dataset Cassandra").master("local").getOrCreate();
		//product_data_raw,attribute_details
		SparkConf conf= new SparkConf().set("spark.driver.memory", "4g").setAppName("Cassandra_Connection").setMaster("local[10]");
		conf.set("spark.cassandra.connection.host", "54.169.17.194");
        conf.set("spark.cassandra.auth.username", "bagira");          
        conf.set("spark.cassandra.auth.password", "Welc0M@2dju9gl@");
        
        JavaSparkContext jsc=new JavaSparkContext(conf);
        
        System.out.println(jsc.appName());
        
        SQLContext sqlcontext= new SQLContext(jsc);
       Dataset<Row>  cassdf=sqlcontext.read().format("org.apache.spark.sql.cassandra").option("table", "product_data_raw").option("keyspace", "products").load();
        
       cassdf.printSchema();
       System.out.println(cassdf.rdd().getNumPartitions());
       cassdf.createOrReplaceTempView("QACass");
       
       //Dataset<Row>  savecassdf= sqlcontext.sql("select id_attribute, attribute_name from QACass where attribute_name IN ('quantity uom', 'UOM', 'uom', 'Items in Pack')");
       //Dataset<Row>  savecassdf= sqlcontext.sql("select attribute_name, count(*) from QACass group by attribute_name order by count(*) DESC");
       Dataset<Row>  savecassdf= sqlcontext.sql("select brand, count(*) from QACass group by brand order by count(*) DESC");
       //System.out.println(savecassdf.rdd().getNumPartitions());
       savecassdf.show();
       //savecassdf.write().format("csv").save("/home/moglix/Desktop/Amit/Data_Spark/QA_Cass_AttributeByNameAndCount");
        jsc.close();
	}
}
