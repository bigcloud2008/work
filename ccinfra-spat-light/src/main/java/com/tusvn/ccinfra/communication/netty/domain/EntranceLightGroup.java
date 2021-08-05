package com.tusvn.ccinfra.communication.netty.domain;

import java.util.List;


/////-----------进口灯色状态信息列表
public class EntranceLightGroup {
	
	private Integer entranceDirection=0; /////进口方向
	
	private Integer entranceGroupNum=0;  ////进口灯组数量
	
	private List<LightGroupColor> groupColors=null; /////灯组灯色信息列表
	
	
	public Integer getEntranceDirection() {
		return entranceDirection;
	}

	public void setEntranceDirection(Integer entranceDirection) {
		this.entranceDirection = entranceDirection;
	}

	public Integer getEntranceGroupNum() {
		return entranceGroupNum;
	}

	public void setEntranceGroupNum(Integer entranceGroupNum) {
		this.entranceGroupNum = entranceGroupNum;
	}

	public List<LightGroupColor> getGroupColors() {
		return groupColors;
	}

	public void setGroupColors(List<LightGroupColor> groupColors) {
		this.groupColors = groupColors;
	}



	public EntranceLightGroup() {
		// TODO Auto-generated constructor stub
	}

}
