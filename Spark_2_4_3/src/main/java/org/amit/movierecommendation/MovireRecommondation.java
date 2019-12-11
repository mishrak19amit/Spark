package org.amit.movierecommendation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MovireRecommondation {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("JavaALSExample").master("local[*]").getOrCreate();

		JavaRDD<Rating> ratingsRDD = spark.read()
				.textFile("/home/moglix/Desktop/Amit/PGitHub/Spark/data/sample_movielens_ratings.txt").javaRDD()
				.map(x -> {
					return parseToRating(x);
				});

		ratingsRDD.take(10).forEach(x -> System.out.println(x.getMovieId()));

		Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
		Dataset<Row>[] splits = ratings.randomSplit(new double[] { 0.8, 0.2 });
		Dataset<Row> training = splits[0];
		Dataset<Row> test = splits[1];

		ALS als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId")
				.setRatingCol("rating");
		ALSModel model = als.fit(training);
		model.setColdStartStrategy("drop");
		Dataset<Row> predictions = model.transform(test);
		RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating")
				.setPredictionCol("prediction");
		Double rmse = evaluator.evaluate(predictions);
		System.out.println("Root-mean-square error = " + rmse);
		
		// Generate top 10 movie recommendations for each user
		Dataset<Row> userRecs = model.recommendForAllUsers(10);
		// Generate top 10 user recommendations for each movie
		Dataset<Row> movieRecs = model.recommendForAllItems(10);

		// Generate top 10 movie recommendations for a specified set of users
		Dataset<Row> users = ratings.select(als.getUserCol()).distinct().limit(3);
		Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users, 10);
		// Generate top 10 user recommendations for a specified set of movies
		Dataset<Row> movies = ratings.select(als.getItemCol()).distinct().limit(3);
		Dataset<Row> movieSubSetRecs = model.recommendForItemSubset(movies, 10);
		
		userRecs.printSchema();
		
		userRecs.show();
		
		//userRecs.repartition(1).write().format("JSON").save("/home/moglix/Desktop/Amit/Data_Spark/MovieRecommendation");
	}

	public static Rating parseToRating(String str) {
		String[] fields = str.split("::");
		if (fields.length != 4) {
			throw new IllegalArgumentException("Each line must contain 4 fields");
		}
		int userId = Integer.parseInt(fields[0]);
		int movieId = Integer.parseInt(fields[1]);
		float rating = Float.parseFloat(fields[2]);
		long timestamp = Long.parseLong(fields[3]);
		return new Rating(userId, movieId, rating, timestamp);
	}	
}
