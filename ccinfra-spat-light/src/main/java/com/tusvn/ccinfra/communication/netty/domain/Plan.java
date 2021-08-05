package com.tusvn.ccinfra.communication.netty.domain;

import java.util.List;


/////----表A.26　信号方案色步消息内容
public class Plan {
 
	private Integer regionId  =null;
	private Integer roadId    =null;
 
	private Long   timestamp=0L;
	
	
	
	private Float lon=null;
	private Float lat=null;
	private Integer alt=0;
	
	private Integer lightGroupNum=0;  ///灯组总数
	private Integer entranceNum  =0;  ///进口数量
	private List<PlanLightGroup> groups=null; /////灯组色步信息列表
 

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

	public Integer getLightGroupNum() {
		return lightGroupNum;
	}

	public void setLightGroupNum(Integer lightGroupNum) {
		this.lightGroupNum = lightGroupNum;
	}

	public Integer getEntranceNum() {
		return entranceNum;
	}

	public void setEntranceNum(Integer entranceNum) {
		this.entranceNum = entranceNum;
	}

	public List<PlanLightGroup> getGroups() {
		return groups;
	}

	public void setGroups(List<PlanLightGroup> groups) {
		this.groups = groups;
	}



	public Plan() {
		// TODO Auto-generated constructor stub
	}

}
