package org.amit.dataSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataSetCassandraFetching {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().set("spark.driver.memory", "4g").setAppName("CategoryDataSetCassandra")
				.setMaster("local[10]");
		conf.set("spark.cassandra.connection.host", "54.169.1.174");
		conf.set("spark.cassandra.auth.username", "bagira");
		conf.set("spark.cassandra.auth.password", "Welc0M@2dju9gl@");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		System.out.println(jsc.appName());

//		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(jsc);
//		CassandraJavaRDD<CassandraRow> rdd = functions.cassandraTable("products", "product_data");
//		rdd.cache();
//		
//		rdd.take(10).forEach(x->{
//			IndexedSeq<String> indexsequence=	x.fieldNames().columnNames();
//			System.out.println(indexsequence);
//			System.out.println(x.dataAsString());
//		
//		});
//
//		
//		JavaRDD<Product> productObj=rdd.map(x->{
//
//			Product product = new Product();
//			
//			scala.collection.immutable.List<String> headerList=x.fieldNames().columnNames().toList();
//			
//			return product;
//		});
//		
//		productObj.take(10).forEach(x->{
//			System.out.println(x.getIdProduct());
//		});
//		
		
		SQLContext sqlcontext = new SQLContext(jsc);
		Dataset<Row> cassdf = sqlcontext.read().format("org.apache.spark.sql.cassandra").option("table", "product_data")
				.option("keyspace", "products").load();

		cassdf.printSchema();
		System.out.println(cassdf.rdd().getNumPartitions());
		cassdf.createOrReplaceTempView("QACassCat");

		// Dataset<Row> savecassdf= sqlcontext.sql("select id_attribute, attribute_name
		// from QACass where attribute_name IN ('quantity uom', 'UOM', 'uom', 'Items in
		// Pack')");
		// Dataset<Row> savecassdf= sqlcontext.sql("select attribute_name, count(*) from
		// QACass group by attribute_name order by count(*) DESC");
		Dataset<Row> savecassdf = sqlcontext.sql("select id_product, manually_grouped as manuallyGrouped, active from QACassCat");
		
		savecassdf.printSchema();
		
		Encoder<ProductCheck> productEncoder = Encoders.bean(ProductCheck.class); 
		System.out.println(productEncoder.schema());
		
		Dataset<ProductCheck> personDF = savecassdf.as(productEncoder);
		personDF.show();
		
		// System.out.println(savecassdf.rdd().getNumPartitions());
//		savecassdf.show();
//		savecassdf.repartition(1).write().format("JSON").save("/home/moglix/Desktop/Amit/Data_Spark/Prod_Cass_Product");
		jsc.close();
	}
}
