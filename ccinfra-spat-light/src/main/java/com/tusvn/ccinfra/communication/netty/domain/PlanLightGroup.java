package com.tusvn.ccinfra.communication.netty.domain;

import java.util.List;

public class PlanLightGroup {
	
	private int entranceDirection;
	private int entranceGroupNum;
	private List<PlanColorInfo>  colorInfos;
	
	public int getEntranceDirection() {
		return entranceDirection;
	}

	public void setEntranceDirection(int entranceDirection) {
		this.entranceDirection = entranceDirection;
	}

	public int getEntranceGroupNum() {
		return entranceGroupNum;
	}

	public void setEntranceGroupNum(int entranceGroupNum) {
		this.entranceGroupNum = entranceGroupNum;
	}

	public List<PlanColorInfo> getColorInfos() {
		return colorInfos;
	}

	public void setColorInfos(List<PlanColorInfo> colorInfos) {
		this.colorInfos = colorInfos;
	}



	public PlanLightGroup() {
		// TODO Auto-generated constructor stub
	}

}
