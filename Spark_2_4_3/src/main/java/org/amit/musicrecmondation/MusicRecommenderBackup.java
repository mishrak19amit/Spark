package org.amit.musicrecmondation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.recommendation.ALS.Rating;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class MusicRecommenderBackup {
	private static final int NUMPARTITION = 8;

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Music playground").setMaster("local[*]");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> rawUserArtist = jsc.textFile(
				"/home/moglix/Desktop/Amit/Data_Spark/MusicRecommDataSet/user_artist_data.txt", NUMPARTITION);

		rawUserArtist.persist(StorageLevel.MEMORY_AND_DISK());

		JavaRDD<String> rawArtist = jsc
				.textFile("/home/moglix/Desktop/Amit/Data_Spark/MusicRecommDataSet/artist_data.txt", NUMPARTITION);

		rawArtist.persist(StorageLevel.MEMORY_AND_DISK());

		JavaRDD<String> rawArtistAlias = jsc.textFile(
				"/home/moglix/Desktop/Amit/Data_Spark/MusicRecommDataSet/artist_alias.txt", NUMPARTITION);

		rawArtistAlias.persist(StorageLevel.MEMORY_AND_DISK());

		JavaPairRDD<Integer, String> artistByID = rawArtist.mapToPair(x -> {

			try {
				String[] vals = x.split("\\t");
				int ID = Integer.parseInt(vals[0]);
				return new Tuple2<Integer, String>(ID, vals[1]);
			} catch (Exception e) {
				System.out.println("Number forma exception " + e.getMessage());
			}
			return null;
		});

		artistByID.persist(StorageLevel.MEMORY_AND_DISK());

		artistByID.take(10).forEach(x -> System.out.println(x._1() + " name: " + x._2()));
		System.out.println("rawUserArtists.count:" + rawUserArtist.count());
		
		JavaPairRDD<Integer, Integer> artistAlias = rawArtistAlias.mapToPair(x -> {

			try {
				String[] vals = x.split("\t");

				int token1 = Integer.parseInt(vals[0]);
				int token2 = Integer.parseInt(vals[1]);

				return new Tuple2<Integer, Integer>(token1, token2);
			} catch (Exception e) {
				System.out.println("Number forma exception " + e.getMessage());
			}
			return null;

		});

		artistAlias.persist(StorageLevel.MEMORY_AND_DISK());

		artistAlias.take(10).forEach(x -> System.out.println(x._1() + " name: " + x._2()));
		System.out.println("rawArtistAlias.count:" + rawArtistAlias.count());
		
		Broadcast< JavaPairRDD<Integer, Integer>> bArtistAlias= jsc.broadcast(artistAlias);
		
		rawUserArtist.count();
		
		JavaRDD<Rating> trainData = rawUserArtist.map(x->{
			
			String[] vals= x.split(" ");
			try {
				int userID=Integer.parseInt(vals[0]);
				int artistID=Integer.parseInt(vals[1]);
				int count=Integer.parseInt(vals[2]);
//				List<Integer> Data=bArtistAlias.getValue().lookup(userID);
//				artistID=Data.get(0);
				return new Rating(userID, artistID, count);
			}
			catch(Exception e) {
				System.out.println("Exception :"+ e.getMessage());
			}
			return null;
			
		});
		
		System.out.println(trainData.count());
		
		trainData.take(10).forEach(x->System.out.println(x.user()));
		
		jsc.close();

	}

}
