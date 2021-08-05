package com.tusvn.ccinfra.communication.netty.domain;

import java.util.List;

public class LaneControlStatus {
	
	private String  msgType   ="laneControlStatus";
	private Integer roadId    =null;
	
	private Integer regionId  =null;
 
	private Long   timestamp=0L;
	
	
	
	private Float lon=null;
	private Float lat=null;
	
	private int alt=0;
	
	private int laneNum=0;
 
	private List<LaneControler> laneControlers;
	
	public String getMsgType() {
		return msgType;
	}

	public void setMsgType(String msgType) {
		this.msgType = msgType;
	}

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

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Float getLon() {
		return lon;
	}

	public void setLon(Float lon) {
		this.lon = lon;
	}

	public Float getLat() {
		return lat;
	}

	public void setLat(Float lat) {
		this.lat = lat;
	}

	public int getAlt() {
		return alt;
	}

	public void setAlt(int alt) {
		this.alt = alt;
	}

	public int getLaneNum() {
		return laneNum;
	}

	public void setLaneNum(int laneNum) {
		this.laneNum = laneNum;
	}

	public List<LaneControler> getLaneControlers() {
		return laneControlers;
	}

	public void setLaneControlers(List<LaneControler> laneControlers) {
		this.laneControlers = laneControlers;
	}



	public LaneControlStatus() {
		// TODO Auto-generated constructor stub
	}

}
