package com.tusvn.ccinfra.communication.netty.domain;

import java.util.List;

/////////////--- 灯色状态
public class EntranceColor {
	
	private Integer regionId  =null;
	private Integer roadId    =null;
	
	
 
	private Long   timestamp=0L;
	
	
	
	private Float lon=null;
	private Float lat=null;
	private Integer alt=0;
	private Integer entranceNum=0;               /////路口进口数量
	private List<EntranceLightGroup> groups=null;/////进口灯色状态列表
	
	
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

	public Integer getAlt() {
		return alt;
	}

	public void setAlt(Integer alt) {
		this.alt = alt;
	}

	public Integer getEntranceNum() {
		return entranceNum;
	}

	public void setEntranceNum(Integer entranceNum) {
		this.entranceNum = entranceNum;
	}

	public List<EntranceLightGroup> getGroups() {
		return groups;
	}

	public void setGroups(List<EntranceLightGroup> groups) {
		this.groups = groups;
	}



	public EntranceColor() {
		// TODO Auto-generated constructor stub
	}

}
