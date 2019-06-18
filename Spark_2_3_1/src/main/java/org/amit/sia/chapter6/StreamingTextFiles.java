package org.amit.sia.chapter6;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

		buyandSell.foreachRDD(x -> {
			x.foreach(xx -> {
				logger.info(xx._1() + "  B/S and it's count " + xx._2());
			});
		});

		JavaPairDStream<String, Integer> stocksPerWindow = ordersDstream.mapToPair(x -> {

			return new Tuple2<String, Integer>(x.getSymbol(), x.getAmount());
		}).window(Durations.seconds(60)).reduceByKey((a, b) -> a + b);

		JavaPairDStream<Integer, String> topStocks = stocksPerWindow.mapToPair(x -> {
			return new Tuple2<Integer, String>(x._2(), x._1());
		}).transformToPair(x -> x.sortByKey(false));

		JavaDStream<List<String>> securities = topStocks.map(x -> x._2()).glom().map(xx -> {
			List<String> evenIndexedNames = IntStream.range(0, xx.size()).filter(i -> i < 5).mapToObj(i -> xx.get(i))
					.collect(Collectors.toList());

			return evenIndexedNames;
		});

		JavaPairDStream<String, String> securitiespaired = topStocks.repartition(1).map(x -> x._2()).glom()
				.mapToPair(xx -> {
					List<String> evenIndexedNames = IntStream.range(0, xx.size()).filter(i -> i < 5)
							.mapToObj(i -> xx.get(i)).collect(Collectors.toList());
					String securitiesnames = "";
					for (String ssc : evenIndexedNames) {
						securitiesnames = securitiesnames + ssc + ",";
					}

					return new Tuple2<String, String>("Top Five Securities: ", securitiesnames);
				});

		securitiespaired.foreachRDD(x -> {
			x.foreach(xx -> logger.info(xx._1() + " " + xx._2()));
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
		}).transformToPair(x -> x.sortByKey(false));


		JavaPairDStream<String, String> top5Clients = swapppedamountState.repartition(1).glom().mapToPair(x -> {
			List<Tuple2<Double, Integer>> evenIndexedNames = IntStream.range(0, x.size()).filter(i -> i < 5)
					.mapToObj(i -> x.get(i)).collect(Collectors.toList());
			String top5clientdetails = "";
			for (Tuple2<Double, Integer> t : evenIndexedNames) {
				top5clientdetails = top5clientdetails + t._2() + " expense: " + t._1() + ",";
			}

			return new Tuple2<String, String>("Top 5 clients", top5clientdetails);

		});
		
		top5Clients.foreachRDD(x->{
			x.foreach(xx->{
				System.out.println(xx._1()+" "+xx._2());
				logger.info(xx._1()+" "+xx._2());
			});
		});
		

		JavaPairDStream<String, String> byandsellunion=buyandSell.mapToPair(x->{
			return new Tuple2<String, String>(x._1(),x._2().toString());
		});
		
		byandsellunion=byandsellunion.union(securitiespaired).union(top5Clients);
		
		byandsellunion.repartition(1).dstream().saveAsTextFiles(
				"/home/moglix/Desktop/Amit/Data_Spark/first-edition-master/ch06/SparkOutPut/OutPut", "text");
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
