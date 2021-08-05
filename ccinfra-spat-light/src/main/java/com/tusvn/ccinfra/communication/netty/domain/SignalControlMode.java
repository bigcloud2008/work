package com.tusvn.ccinfra.communication.netty.domain;

public class SignalControlMode {
	
	private Integer regionId  =null;
	public Integer getRegionId() {
		return regionId;
	}

	public void setRegionId(Integer regionId) {
		this.regionId = regionId;
	}

	public Integer getRoadId() {
		return roadId;
	}

	public void setRoadId(Integer roadId) {
		this.roadId = roadId;
	}



	private Integer roadId    =null;
	
	private float longitude;
	private float latitude;
	private int   altitude;
	private long  time;
	private int   mode;
	
	
	public float getLongitude() {
		return longitude;
	}

	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}

	public float getLatitude() {
		return latitude;
	}

	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}

	public int getAltitude() {
		return altitude;
	}

	public void setAltitude(int altitude) {
		this.altitude = altitude;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public int getMode() {
		return mode;
	}

	public void setMode(int mode) {
		this.mode = mode;
	}



	public SignalControlMode(Integer regionId, Integer roadId, float longitude, float latitude, int altitude, long  time, int mode) {
		// TODO Auto-generated constructor stub
		this.regionId=regionId;
		
		this.roadId=roadId;
		
		this.longitude=longitude;
		this.latitude=latitude;
		this.altitude=altitude;
		this.time=time;
		this.mode=mode;
		
	}

}
