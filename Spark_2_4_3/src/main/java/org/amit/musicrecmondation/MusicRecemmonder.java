package org.amit.musicrecmondation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MusicRecemmonder {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("JavaALSExample_MusciRecemmonder").master("local[*]")
				.getOrCreate();

		createArtistView(spark);
		
		JavaRDD<MusicRating> ratingsRDD = spark.read()
				.textFile("/home/moglix/Desktop/Amit/Data_Spark/MusicRecommDataSet/user_artist_data.txt").javaRDD()
				.map(x -> {
					return parseToRating(x);
				});

		System.out.println(ratingsRDD.count());

		ratingsRDD.take(10)
				.forEach(x -> System.out.println(x.getUserId() + " " + x.getArtistId() + " " + x.getCount()));

		Dataset<Row> musicDataset = spark.createDataFrame(ratingsRDD, MusicRating.class);
		Dataset<Row>[] splits = musicDataset.randomSplit(new double[] { 0.8, 0.2 });
		Dataset<Row> trainDataset = splits[0];
		Dataset<Row> testDataset = splits[1];

		ALS als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("artistId")
				.setRatingCol("count");

		ALSModel model = als.fit(trainDataset);
		model.setColdStartStrategy("drop");
		Dataset<Row> predictions = model.transform(testDataset);

		RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("count")
				.setPredictionCol("prediction");
		Double rmse = evaluator.evaluate(predictions);
		System.out.println("Root-mean-square error = " + rmse);

		// Generate top 10 music recommendations for each user
		Dataset<Row> userRecs = model.recommendForAllUsers(10);
//		// Generate top 10 user recommendations for each music
//		Dataset<Row> musicRecs = model.recommendForAllItems(10);
//
//		// Generate top 10 music recommendations for a specified set of users
//		Dataset<Row> users = musicDataset.select(als.getUserCol()).distinct().limit(3);
//		Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users, 10);
//		// Generate top 10 user recommendations for a specified set of musics
//		Dataset<Row> musics = musicDataset.select(als.getItemCol()).distinct().limit(3);
//		Dataset<Row> musicSubSetRecs = model.recommendForItemSubset(musics, 10);
//		
		
		///home/moglix/Desktop/Amit/Data_Spark/MovieRecommendation/Music
		
		userRecs.repartition(1).write().format("JSON").save("/home/moglix/Desktop/Amit/Data_Spark/MovieRecommendation/Music");
		
	}

	private static void createArtistView(SparkSession spark) {
		JavaRDD<Artist> artistRDD = spark.read()
				.textFile("/home/moglix/Desktop/Amit/Data_Spark/MusicRecommDataSet/artist_data.txt").javaRDD()
				.map(x -> {
					

					String[] vals= x.split("\t");
					
					try {
						
						int artistID=Integer.parseInt(vals[0]);
						String artistName=vals[1];
						
						return new Artist(artistID, artistName);
						
					}
					catch(Exception e) {
						System.out.println("Exception "+e.getMessage());
					}
					
					return new Artist();
				});
		
		System.out.println(artistRDD.count());
		Dataset<Row> artistDataset = spark.createDataFrame(artistRDD, Artist.class);
		artistDataset.printSchema();
		
		artistDataset.createOrReplaceTempView("Artist");
	}

	public static MusicRating parseToRating(String str) {
		String[] fields = str.split(" ");
		if (fields.length != 3) {
			System.out.println("Each line must contain 3 fields");
			return new MusicRating();
		}
		int userId = Integer.parseInt(fields[0]);
		int artistId = Integer.parseInt(fields[1]);
		float count = Float.parseFloat(fields[2]);
		return new MusicRating(userId, artistId, count);
	}

}
