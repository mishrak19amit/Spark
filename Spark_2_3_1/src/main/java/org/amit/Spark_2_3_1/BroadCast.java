package org.amit.Spark_2_3_1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadCast {

	public static void main(String[] args) {
		SparkConf conf= new SparkConf().setAppName("broadCast").setMaster("local");
		JavaSparkContext jsc= new JavaSparkContext(conf);
		int[] arr= new int[] {1,2,3};
		Broadcast<int[]> broadcastvar= jsc.broadcast(arr);
		int [] arrval=broadcastvar.value();
		
		for(int a:arrval)
			System.out.println(a);

		jsc.close();
	}
}
