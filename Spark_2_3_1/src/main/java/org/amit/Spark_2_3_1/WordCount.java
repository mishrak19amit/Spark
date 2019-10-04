package org.amit.Spark_2_3_1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Word_Count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// /var/log/moglix/platform/dataload
		// /home/moglix/Desktop/Amit/PGitHub/Spark/data/Sample_Word_Count_Data.txt
		JavaRDD<String> lines = sc.textFile("/home/moglix/Desktop/Amit/Data_Spark/first-edition/README.md");

		JavaRDD<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordpair = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1));

		JavaPairRDD<String, Integer> wordcount = wordpair.reduceByKey((x, y) -> x + y);
		JavaPairRDD<Integer, String> swappedwordcount = wordcount.mapToPair(x -> x.swap()).sortByKey(false);

		wordcount = swappedwordcount.mapToPair(x -> x.swap());
		JavaRDD<String> wordcountInfo = words.filter(x -> x.contains("Debug"));
		JavaRDD<String> wordcountError = words.filter(x -> x.contains("INFO"));
		System.out.println("Total number of Info :" + wordcountInfo.count());
		System.out.println("Total number of Error :" + wordcountError.count());
		wordcount = wordcount.filter(x -> 1000 < x._2());
		wordcount.foreach(x -> System.out.println(x._1() + " --> " + x._2()));
		System.out.println("Amit Mishra");

		SparkSession scSession = getSparkSession(wordcountInfo.context());

		getDataSet(scSession);
		
		scSession.sql("select * from Dataset").show();

		sc.close();
	}

	private static SparkSession getSparkSession(SparkContext context) {
		return SparkSession.builder().config(context.conf()).getOrCreate();
	}
	
	private static void getDataSet(SparkSession sc)
	{
		 StructField[] structFields = new StructField[]{
		            new StructField("intColumn", DataTypes.IntegerType, true, Metadata.empty()),
		            new StructField("stringColumn", DataTypes.StringType, true, Metadata.empty())
		    };
		 
		 StructField[] demoStructField= new StructField[] {
				 new StructField("name", DataTypes.StringType, true, Metadata.empty()),
				 new StructField("age", DataTypes.IntegerType, true, Metadata.empty())
		 };
		 
		 StructType demoStructType=new StructType(demoStructField);
		 
		 List<Row> demorows=new ArrayList<Row>();
		 
		 demorows.add(RowFactory.create("Amit", 24));
		 demorows.add(RowFactory.create("Ramesht", 25));
		 
		 Dataset<Row> demoDataset= sc.createDataFrame(demorows, demoStructType);
		 
		 demoDataset.printSchema();
		 demoDataset.show();

		    StructType structType = new StructType(structFields);

		    List<Row> rows = new ArrayList<>();
		    rows.add(RowFactory.create(1, "v1"));
		    rows.add(RowFactory.create(2, "v2"));

		    Dataset<Row> df = sc.createDataFrame(rows, structType);
		    df.printSchema();
		    df.createOrReplaceTempView("Dataset");
		    
	}

}
