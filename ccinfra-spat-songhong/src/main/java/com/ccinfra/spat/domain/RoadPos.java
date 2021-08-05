package com.ccinfra.spat.domain;
/**
 * 道路信息,与mysql对应
 * @author tsing
 *
 */
public class RoadPos {

	private int roadId;
	private int regionId;
	private double lon;
	private double lat;
	private int radius;
	
	public RoadPos() {
		super();
	}

	public int getRoadId() {
		return roadId;
	}
	public void setRoadId(int roadId) {
		this.roadId = roadId;
	}
	
	public int getRegionId() {
		return regionId;
	}
	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}
	public double getLon() {
		return lon;
	}
	public void setLon(double lon) {
		this.lon = lon;
	}
	public double getLat() {
		return lat;
	}
	public void setLat(double lat) {
		this.lat = lat;
	}
	public int getRadius() {
		return radius;
	}
	public void setRadius(int radius) {
		this.radius = radius;
	}
	
}
