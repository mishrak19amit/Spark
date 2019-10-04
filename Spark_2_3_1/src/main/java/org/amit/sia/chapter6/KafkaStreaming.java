package org.amit.sia.chapter6;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaStreaming {

	private static final Logger logger = LogManager.getLogger(KafkaStreaming.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("KafkaStreaming_SIA_06").setMaster("local[10]");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(10));

		try {
			Map<String, Object> consumerkafkaParams = getConsumerkafkaParam();

			String topic = "orders";
			Set<String> topics = new HashSet<>(Arrays.asList(topic.split(" ")));
			JavaInputDStream<ConsumerRecord<String, String>> records = KafkaUtils.createDirectStream(sc,
					LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, consumerkafkaParams));
			JavaDStream<String> recodrsds = records.map(x -> x.value());

			sc.checkpoint("/home/moglix/Desktop/Amit/Data_Spark/checkPointDir");
			// Writing records to another kafka topic
			// produceToKakfa(recodrsds);

			// Calling buy and sell
			buyAndSell(recodrsds);

			// Top 5 Client
			topFiveClient(recodrsds);

			// Top 5 Securities
			topFiveSecurities(recodrsds);

			sc.start();
			sc.awaitTermination();
		} catch (Exception e) {
			logger.error(e);
		} finally {
			sc.close();
		}

	}

	public static Map<String, Object> getConsumerkafkaParam() {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "SIA_Chap06");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

		return kafkaParams;

	}

	public static Producer<String, String> getKafkaProducer() {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		kafkaParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		kafkaParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		kafkaParams.put(ProducerConfig.CLIENT_ID_CONFIG, "SIA_chap06CI");
		kafkaParams.put(ProducerConfig.LINGER_MS_CONFIG, "10");

		return new KafkaProducer<>(kafkaParams);
	}

	public static void buyAndSell(JavaDStream<String> recodrsds) {

		JavaDStream<Order> orders = getOrderDstream(recodrsds);

		JavaPairDStream<String, Integer> buyandSell = orders.mapToPair(x -> {

			return new Tuple2<String, Integer>(x.isBuy(), 1);
		}).reduceByKey((a, b) -> a + b);

		buyandSell.repartition(1).foreachRDD(x -> x.foreach(f -> {
			Producer<String, String> producer = getKafkaProducer();
			String buyorSell = "";
			if (f._1().equalsIgnoreCase("S"))
				buyorSell = "SOLD ";
			else
				buyorSell = "BOUGHT ";

			producer.send(new ProducerRecord<String, String>("metrics", "0", buyorSell + ":-> " + f._2().toString()));
			producer.close();
		}));

	}

	public static JavaPairDStream<Integer, Double> updateState(JavaPairDStream<Integer, Double> clients) {
		JavaPairDStream<Integer, Double> updatedClients;

		updatedClients = clients.updateStateByKey((vales, topValues) -> {

			Double newValues = 0.0;
			if (topValues.isPresent())
				newValues = (Double) topValues.get();
			for (Double val : vales)
				newValues += val;
			return Optional.of(newValues);
		});

		return updatedClients;
	}

	public static void topFiveClient(JavaDStream<String> recodrsds) {

		JavaDStream<Order> orders = getOrderDstream(recodrsds);
		JavaPairDStream<Integer, Double> clients = orders.mapToPair(xx -> {
			return new Tuple2<Integer, Double>(xx.getClientId(), xx.getAmount() * xx.getPrice());
		}).reduceByKey((a, b) -> a + b);
		clients = updateState(clients);
		JavaPairDStream<Double, Integer> swappedclients = clients
				.mapToPair(x -> new Tuple2<Double, Integer>(x._2(), x._1())).transformToPair(x -> x.sortByKey(false));
		JavaPairDStream<String, String> topclients = swappedclients.repartition(1).glom().mapToPair(x -> {
			List<Tuple2<Double, Integer>> evenIndexedNames = IntStream.range(0, x.size()).filter(i -> i < 5)
					.mapToObj(i -> x.get(i)).collect(Collectors.toList());
			String top5clientdetails = "";
			for (int i = 0; i < evenIndexedNames.size() - 1; i++) {
				top5clientdetails = top5clientdetails + evenIndexedNames.get(i)._2() + " expense: "
						+ evenIndexedNames.get(i)._1() + ", ";
			}
			top5clientdetails = top5clientdetails + evenIndexedNames.get(evenIndexedNames.size() - 1)._2()
					+ " expense: " + evenIndexedNames.get(evenIndexedNames.size() - 1)._1();

			return new Tuple2<String, String>("Top 5 clients ", top5clientdetails);
		});

		topclients.foreachRDD(x -> x.foreach(xx -> {
			Producer<String, String> producer = getKafkaProducer();
			producer.send(new ProducerRecord<String, String>("metrics", "0", xx._1() + " :" + xx._2()));
			producer.close();
			System.out.println(xx);
		}));

	}

	public static void topFiveSecurities(JavaDStream<String> recodrsds) {

		JavaDStream<Order> orders = getOrderDstream(recodrsds);

		JavaPairDStream<String, Integer> stockPerWindow = orders.mapToPair(x -> {
			return new Tuple2<String, Integer>(x.getSymbol(), x.getAmount());
		}).window(Durations.seconds(60)).reduceByKey((a, b) -> a + b);
		;

		JavaPairDStream<Integer, String> sortedStockPerWindow = stockPerWindow.mapToPair(x -> {
			return new Tuple2<Integer, String>(x._2(), x._1());
		}).transformToPair(x -> x.sortByKey(false));

		JavaPairDStream<String, String> top5Securities = sortedStockPerWindow.repartition(1)
				.map(x -> x._2() + ": Amount (" + x._1() + ")").glom().mapToPair(x -> {

					List<String> firstFive = IntStream.range(0, x.size()).filter(xx -> xx < 5).mapToObj(i -> x.get(i))
							.collect(Collectors.toList());
					String top5SecuritiesSymbol = "";
					int i = 0;
					for (i = 0; i < firstFive.size() - 1; i++) {
						top5SecuritiesSymbol = top5SecuritiesSymbol + firstFive.get(i) + ", ";
					}
					top5SecuritiesSymbol = top5SecuritiesSymbol + firstFive.get(i);

					return new Tuple2<String, String>("Top Five Securities: ", top5SecuritiesSymbol);
				});

		top5Securities.foreachRDD(x -> x.foreach(xx -> {
			Producer<String, String> producer = getKafkaProducer();
			producer.send(new ProducerRecord<String, String>("metrics", "0", xx._1() + "" + xx._2()));
			producer.close();
			System.out.println(xx._1() + " " + xx._2());
		}));
	}

	public static JavaDStream<Order> getOrderDstream(JavaDStream<String> recodrsds) {
		return recodrsds.map(x -> {
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
	}

	public static void produceToKakfa(JavaDStream<String> recodrsds) {

		recodrsds.foreachRDD(f -> f.foreachPartition(x -> {
			System.out.println("Writing Data to kafka...");
			Producer<String, String> producer = getKafkaProducer();
			while (x.hasNext()) {
				System.out.println(x.next());
				producer.send(new ProducerRecord<String, String>("metrics", "0", x.next()));
			}
			producer.close();
			System.out.println("Data written to kafka!");
		}));

	}
}
