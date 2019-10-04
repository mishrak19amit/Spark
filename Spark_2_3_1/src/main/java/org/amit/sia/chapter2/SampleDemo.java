package org.amit.sia.chapter2;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SampleDemo {
	
	public static void main(String[] args) {
		SparkConf conf= new SparkConf().setAppName("Sample_Demo").setMaster("local[*]");
		JavaSparkContext sc= new JavaSparkContext(conf);
		//List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> listData = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		
		listData.foreach(x->System.out.println(x));
		
		JavaRDD<Integer> sampleData =listData.sample(false, 0.2);
		
		sampleData.foreach(x->System.out.println(x));
		
		List<Integer> takeSampleData =listData.takeSample(false, 2);
		
		System.out.println(takeSampleData);
		
		sc.close();
	}

}
