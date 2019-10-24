package org.amit.movierecommendation;

import java.io.Serializable;

public class Rating implements Serializable {
	  private int userId;
	  private int movieId;
	  private float rating;
	  private long timestamp;

	  public Rating() {}

	  public Rating(int userId, int movieId, float rating, long timestamp) {
	    this.userId = userId;
	    this.movieId = movieId;
	    this.rating = rating;
	    this.timestamp = timestamp;
	  }

	  public int getUserId() {
	    return userId;
	  }

	  public int getMovieId() {
	    return movieId;
	  }

	  public float getRating() {
	    return rating;
	  }

	  public long getTimestamp() {
	    return timestamp;
	  }
}