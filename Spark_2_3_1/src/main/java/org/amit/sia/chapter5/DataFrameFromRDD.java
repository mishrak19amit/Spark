package org.amit.sia.chapter5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple13;

public class DataFrameFromRDD {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CreatinDataFrameFromRDD_SIA_05").setMaster("local[10]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		try {
			JavaRDD<String> lines = sc
					.textFile("/home/moglix/Desktop/Amit/Data_Spark/first-edition-master/ch05/italianPosts.csv");
			System.out.println(lines.count());

			JavaRDD<Tuple13<String, String, String, String, String, String, String, String, String, String, String, String, String>> col13Rdd = lines
					.map(x -> {
						String[] values = x.split("~");
						return new Tuple13<String, String, String, String, String, String, String, String, String, String, String, String, String>(
								values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7],
								values[8], values[9], values[10], values[11], values[12]);
					});

			col13Rdd.take(10).forEach(x -> System.out.println(x));
 
			
			System.out.println(lines.count());

		} catch (Exception e) {
			System.out.println(e);
		} finally {
			sc.close();
		}
	}

}
