package org.amit.musicrecmondation;

import java.io.Serializable;

public class MusicRating implements Serializable {
	private int userId;
	private int artistId;
	private float count;

	public MusicRating() {
	}

	public MusicRating(int userId, int artistId, float count) {
		this.userId = userId;
		this.artistId = artistId;
		this.count = count;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public int getArtistId() {
		return artistId;
	}

	public void setArtistId(int artistId) {
		this.artistId = artistId;
	}

	public float getCount() {
		return count;
	}

	public void setCount(float count) {
		this.count = count;
	}

}