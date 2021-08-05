package com.tusvn.ccinfra.communication.netty.domain;

import java.util.List;

public class LaneFunc {
	
	private int inDirection;
	private int inLaneNum;
	private List<Integer> laneInfos;
	
	public int getInDirection() {
		return inDirection;
	}

	public void setInDirection(int inDirection) {
		this.inDirection = inDirection;
	}

	public int getInLaneNum() {
		return inLaneNum;
	}

	public void setInLaneNum(int inLaneNum) {
		this.inLaneNum = inLaneNum;
	}

	public List<Integer> getLaneInfos() {
		return laneInfos;
	}

	public void setLaneInfos(List<Integer> laneInfos) {
		this.laneInfos = laneInfos;
	}

	

	public LaneFunc() {
		// TODO Auto-generated constructor stub
	}

}
