package org.amit.cassandradataset;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FileDataTransformation {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("FileTransformation").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> filedata = sc.textFile("/home/moglix/Downloads/failedList1", 2)
				.flatMap(x -> Arrays.asList(x.split(",")).iterator());

		JavaRDD<String> commaLine = filedata.map(x -> {
			return "\"" + x.replace(" ", "").replace("\"", "") + "\" ,";
		});

		commaLine.take(10).forEach(x -> {
			System.out.println(x);
		});
		commaLine.repartition(1).saveAsTextFile("/home/moglix/Desktop/Amit/Data_Spark/FileFormat");

		sc.close();

	}

}
