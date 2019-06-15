package org.amit.sia.chapter4;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class JoiningSortingGrouping {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JoiningSortingGrouping_SIA_Chapter04").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		try {
			JavaRDD<String> lines = sc
					.textFile("/home/moglix/Desktop/Amit/Data_Spark/first-edition-master/ch04/ch04_data_transactions.txt");
			JavaPairRDD<Integer, String> pairrdd = lines.mapToPair(x -> {
				int key = Integer.parseInt(x.split("#")[3]);
				return new Tuple2<>(key, x);

			});
			
			Map<Integer, Long> itemsold=pairrdd.countByKey();
			
			
			JavaPairRDD<Integer, Integer> itemWithCountreverse =pairrdd.mapToPair(x->{
				int quantity=Integer.parseInt(x._2().split("#")[4]);
				return new Tuple2<Integer, Integer>(x._1(), quantity);
			}).mapToPair(x-> new Tuple2<Integer, Integer>(x._2(), x._1())).sortByKey(false);
			
			JavaPairRDD<Integer, Integer> itemWithCount=itemWithCountreverse.mapToPair(x-> new Tuple2<Integer, Integer>(x._2(),x._1()));
			itemWithCount=itemWithCount.reduceByKey((a,b)->a+b);
			itemWithCount.take(20).forEach(x->System.out.println(x));
			System.out.println(itemWithCount.count());
			int key=getMostTransaction(itemsold);
			System.out.println(itemsold.get(key)+" user bought item with ItemID: "+key);
			
		}catch(Exception e)
		{
			System.out.println(e);
		}
		finally {
			sc.close();
		}
	}
	
	public static int getMostTransaction(Map<Integer, Long> valuMap) {
		int max = 0;
		int valueKey = -1;
		for (int key : valuMap.keySet()) {
			int value = valuMap.get(key).intValue();
			if (max < value) {
				max = value;
				valueKey = key;
			}
		}

		return valueKey;
	}

}
