package org.amit.cassandradatafetch;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class CategoryDataFetch {

	public static void main(String[] args) {
		//category_details, product_data
		//18.139.106.59, 13.234.108.103
		SparkConf conf = new SparkConf().set("spark.driver.memory", "4g").setAppName("CategoryDataSetCassandra")
				.setMaster("local[10]");
		conf.set("spark.cassandra.connection.host", "18.139.106.59");
		conf.set("spark.cassandra.auth.username", "bagira");
		conf.set("spark.cassandra.auth.password", "Welc0M@2dju9gl@");

		SparkSession sc = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> cassdf = sc.read().format("org.apache.spark.sql.cassandra").option("table", "category_details")
				.option("keyspace", "products").load();

		cassdf.printSchema();

		cassdf.createOrReplaceTempView("ProdCassCat");

		Dataset<Row> savecassdf = sc.sql("select category_name from ProdCassCat");

		JavaRDD<Row> categoryDet=savecassdf.javaRDD();
		
		JavaPairRDD<String, Integer> categoryCount=categoryDet.mapToPair(x->{
			
			String cat=x.get(0).toString().replace("& ", "").replace("-", " ");
			String[] vals= cat.split(" ");
			
			return new Tuple2<String, Integer>(x.get(0).toString().toLowerCase(), vals.length);
			
		});
		JavaPairRDD<Integer, String> catKeyWordCountSorted=categoryCount.mapToPair(x->{
			return new Tuple2<String,Integer>(x._1(), 1);
		}).reduceByKey((a,b)-> a+b).mapToPair(x->{
			return new Tuple2<Integer, String>(x._2(),x._1());
		}).sortByKey(false);
		
		catKeyWordCountSorted.take(100).forEach(x->{
			System.out.println(x._1()+" "+ x._2());
		});
		
		JavaPairRDD<Integer,String> duplicateCat=categoryCount.mapToPair(x->{
			return new Tuple2<Integer, String>(x._2(), x._1());
		}).sortByKey();
		
		//catKeyWordCountSorted.repartition(1).saveAsTextFile("/home/moglix/Desktop/Amit/Data_Spark/categoryKeywordCount");
		
		
		List<String> category6LongKeyword=categoryCount.mapToPair(x->{
			return new Tuple2<Integer, String>(x._2(), x._1());
		}).lookup(3);
		
//		for(String cat: category6LongKeyword) {
//			System.out.println(cat);
//		}
		
//		categoryCount.take(10).forEach(x->{
//			System.out.println("["+x._1()+"] count: "+ x._2());
//		});
		
//		JavaPairRDD<Integer, Integer> categoryCountbyCount= categoryCount.map(x->{
//			return x._2();
//		}).mapToPair(x->{
//			return new Tuple2<Integer, Integer>(x,1);
//		}).reduceByKey((a,b)->a+b);
//		
//		categoryCountbyCount.take(10).forEach(x->{
//			System.out.println(x._1()+" tf "+ x._2());
//		});
		
		//categoryCount.repartition(1).saveAsTextFile("/home/moglix/Desktop/Amit/Data_Spark/categoryKeywordCount");
		
		//savecassdf.printSchema();

		//savecassdf.show();
		//cassdf.repartition(1).write().format("JSON").save("/home/moglix/Desktop/Amit/Data_Spark/Prod_Cass_Cat");
		sc.close();
	}
}
