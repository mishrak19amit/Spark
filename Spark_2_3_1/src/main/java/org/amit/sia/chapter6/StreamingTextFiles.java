package org.amit.sia.chapter6;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingTextFiles {

	private static final Logger logger = LogManager.getLogger(StreamingTextFiles.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TextStream_SIA_06").setMaster("local[10]");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(10));
		JavaDStream<String> lineDstream = sc
				.textFileStream("/home/moglix/Desktop/Amit/Data_Spark/first-edition-master/ch06/SparkInput");

		System.out.println(lineDstream.count());
		sc.checkpoint("/home/moglix/Desktop/Amit/Data_Spark/checkPointDir");
		JavaDStream<Order> ordersDstream = lineDstream.map(x -> {
			Order order = new Order();
			String[] values = x.split(",");
			SimpleDateFormat datetimeFormatter1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			Date lFromDate1;
			try {
				lFromDate1 = datetimeFormatter1.parse(values[0]);
				Timestamp fromTS1 = new Timestamp(lFromDate1.getTime());
				order.setTime(fromTS1);
				order.setOrderId(Integer.parseInt(values[1]));
				order.setClientId(Integer.parseInt(values[2]));
				order.setSymbol(values[3]);
				order.setAmount(Integer.parseInt(values[4]));
				order.setPrice(Double.parseDouble(values[5]));
				order.setBuy(values[6]);
			} catch (ParseException e) {
				System.out.println("Wrong line format (" + e + "): " + x);
			}

			return order;
		});

		JavaPairDStream<String, Integer> buyandSell = ordersDstream.mapToPair(x -> {

			return new Tuple2<String, Integer>(x.isBuy(), 1);
		}).reduceByKey((a, b) -> a + b);

		System.out.println(buyandSell.count());

		buyandSell.foreachRDD(x -> {
			x.foreach(xx -> {
				System.out.println(xx._1() + " " + xx._2());
				logger.info(xx._1() + "  B/S and it's count " + xx._2());
			});
		});

		JavaPairDStream<Integer, Double> amountPerClient = ordersDstream.mapToPair(x -> {

			return new Tuple2<Integer, Double>(x.getClientId(), x.getAmount() * x.getPrice());
		});

		JavaPairDStream<Integer, Double> amountState = amountPerClient.updateStateByKey((vals, totalOpt) -> {
			Double newSum = 0.0;
			if (totalOpt.isPresent())
				newSum = totalOpt.get();
			for (Double i : vals)
				newSum += i;
			return Optional.of(newSum);
		});

		JavaPairDStream<Double, Integer> swapppedamountState = amountState.mapToPair(x -> {
			return new Tuple2<Double, Integer>(x._2(), x._1());
		});

		// JavaPairDStream<Double,Integer> top5clients=
		swapppedamountState.transformToPair(x -> x.sortByKey(false)).foreachRDD(x -> {
			x.foreach(xx -> {
				// System.out.println("Client ID : " + xx._2() + " total Expense " + xx._1());
				logger.info("Client ID : " + xx._2() + " total Expense " + xx._1());

			});
		});

		System.out.println("Amit");

		buyandSell.repartition(1).dstream().saveAsTextFiles(
				"/home/moglix/Desktop/Amit/Data_Spark/first-edition-master/ch06/SparkOutPut/OutPut", "text");
		System.out.println(ordersDstream.count());
		sc.start();
		try {
			sc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println(e);
		} finally {
			sc.close();
		}

	}

}
