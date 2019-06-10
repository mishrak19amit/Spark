package org.amit.kafkaSpark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.datastax.driver.mapping.annotations.Transient;

import scala.Tuple2;

public class WordCountKafkaDataset {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Kafka_Word_COunt").setMaster("local[5]");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		String topic = "AmitTopic";
		Set<String> topics = new HashSet<>(Arrays.asList(topic.split(" ")));
		JavaInputDStream<ConsumerRecord<String, String>> records = KafkaUtils.createDirectStream(jsc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));
		JavaDStream<String> recordvalue = records.map(x -> x.value());
		JavaDStream<String> words = recordvalue.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
				.filter(x -> x.contains("#"));
		words.foreachRDD(kafkarecord -> {
			JavaRDD<JavaRecord> recordRdd = kafkarecord.map(x -> {
				JavaRecord record = new JavaRecord();
				record.setRecord(x);
				return record;
			});

			SparkSession ssc = SparkSessionSingelton.getSparkSession(kafkarecord.context().conf());
			Dataset<Row> javaRecordDF=ssc.createDataFrame(recordRdd, JavaRecord.class);
			
			javaRecordDF.createOrReplaceTempView("JavaRecord");
			
			ssc.sql("select record, count(*) from JavaRecord group by record").show();
			
		});
		jsc.start();
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}

class SparkSessionSingelton {
	private static transient SparkSession instance = null;
	public static SparkSession getSparkSession(SparkConf conf) {
		if(null==instance) {
			instance=SparkSession.builder().config(conf).getOrCreate();
		}

		return instance;
	}
}
