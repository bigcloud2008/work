package com.tusvn.ccinfra.communication.netty.domain;

public class SignalStatus {
	
	private Integer regionId  =null;
	private Integer roadId    =null;
	
	public Integer getRoadId() {
		return roadId;
	}


	public void setRoadId(Integer roadId) {
		this.roadId = roadId;
	}


	public Integer getRegionId() {
		return regionId;
	}


	public void setRegionId(Integer regionId) {
		this.regionId = regionId;
	}





	
	
	private float longitude;
	private float latitude;
	private int   altitude;
	private long  time;
	

	/***
	 * 
	 * 整数，取值范围：
0：无效
1：工作正常
2：故障状态
3：其他

	 * 
	 * **/
	private int   status;
	
	
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


	public int getStatus() {
		return status;
	}


	public void setStatus(int status) {
		this.status = status;
	}



	

	public SignalStatus(Integer regionId, Integer roadId, float longitude, float latitude, int altitude, long  time, int status) {
		// TODO Auto-generated constructor stub
		this.regionId=regionId;
		
		this.roadId=roadId;
		
		this.longitude=longitude;
		this.latitude=latitude;
		this.altitude=altitude;
		this.time=time;
		this.status=status;
	}

}
