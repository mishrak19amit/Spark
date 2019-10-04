package org.amit.cassandradataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MSNFormattingForList {

	public static void main(String[] args) {
		
		SparkConf conf= new SparkConf().setAppName("MSN_Formatting").setMaster("local[*]");
		
		JavaSparkContext sc= new JavaSparkContext(conf);
		
		JavaRDD<String> msnIDs=sc.textFile("/home/moglix/Desktop/Amit/Data_Spark/Prod_MSN.csv");
		
		JavaRDD<String> formattedMsnIDs=msnIDs.map(x->{
			return "\""+x+"\" ,";
		});
		
		formattedMsnIDs.foreach(x->System.out.print(x));
		sc.close();
		
	}
	
}
