package com.tusvn.ccinfra.communication.netty.domain;

public class LaneControler {
	
	private Integer type;////bit0--车道， bit1---进口匝道， bit2--出口匝道   取值1表示具有，0表示不具有
	public Integer getType() {
		return type;
	}


	public void setType(Integer type) {
		this.type = type;
	}


	public int getLaneNo() {
		return laneNo;
	}


	public void setLaneNo(int laneNo) {
		this.laneNo = laneNo;
	}


	public int getLaneDirection() {
		return laneDirection;
	}


	public void setLaneDirection(int laneDirection) {
		this.laneDirection = laneDirection;
	}


	public int getLaneSignal() {
		return laneSignal;
	}


	public void setLaneSignal(int laneSignal) {
		this.laneSignal = laneSignal;
	}


	private int laneNo;
	private int laneDirection;
	private int laneSignal;
	

	public LaneControler() {
		// TODO Auto-generated constructor stub
	}

}
