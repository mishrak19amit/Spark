package org.amit.broadcastandaccumulators;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.sysFuncNames_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

public class DemoBroadcastAccum {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("BroadCastDemo").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		System.out.println(jsc);

		// broadCastAndAccumDemo(jsc);

		//reducDemoOnRDD(jsc);
		reducDemoOnPairRDD(jsc);

		jsc.close();
	}

	private static void reducDemoOnPairRDD(JavaSparkContext jsc) {

		JavaRDD<Integer> javaRDD = getIntegerRDD(jsc);

		JavaPairRDD<Integer, Integer> javaPairRDD = getIntegerPairRDD(javaRDD);

		JavaPairRDD<Integer, Integer> javaReducedPPairRDD=javaPairRDD.reduceByKey((a,b)->a+b).sortByKey(true);
		
		javaReducedPPairRDD.foreach(x->System.out.println(x._1()+" "+x._2()));
		
	}

	private static JavaPairRDD<Integer, Integer> getIntegerPairRDD(JavaRDD<Integer> javaRDD) {

		return javaRDD.mapToPair(x -> {
			return new Tuple2<>(x, x);
		});
	}

	private static JavaRDD<Integer> getIntegerRDD(JavaSparkContext jsc) {
		return jsc.parallelize(Arrays.asList(new Integer[] { 1, 2, 3, 4 }));

	}

	private static void reducDemoOnRDD(JavaSparkContext jsc) {

		JavaRDD<Integer> javaRDD = getIntegerRDD(jsc);

		Integer reducedInt = javaRDD.reduce((a, b) -> a + b);
		System.out.println(reducedInt);
	}

	private static void broadCastAndAccumDemo(JavaSparkContext jsc) {

		Broadcast<int[]> broadcast = jsc.broadcast(new int[] { 1, 2, 3 });

		LongAccumulator accum = jsc.sc().longAccumulator();

		for (int val : broadcast.value()) {
			System.out.println(val);
			accum.add(val);
		}

		System.out.println(accum.value());

	}

}
