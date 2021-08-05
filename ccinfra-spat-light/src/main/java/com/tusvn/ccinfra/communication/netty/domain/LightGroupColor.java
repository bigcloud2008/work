package com.tusvn.ccinfra.communication.netty.domain;

/////////------------灯组灯色信息
public class LightGroupColor {
	
	private int groupNo;  ////灯组编号
	private int groupType;////灯组类型
	private int color;    ////2---红闪， 3----红   5--绿  6--绿闪   7---黄   8--黄闪
	private int status;   ////0---无灯， 1---灭灯  2---亮灯   3----闪烁   -----------------保留用
	private int remainTime; ///////剩余时间
	
	public int getGroupNo() {
		return groupNo;
	}

	public void setGroupNo(int groupNo) {
		this.groupNo = groupNo;
	}

	public int getGroupType() {
		return groupType;
	}

	public void setGroupType(int groupType) {
		this.groupType = groupType;
	}

	public int getColor() {
		return color;
	}

	public void setColor(int color) {
		this.color = color;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getRemainTime() {
		return remainTime;
	}

	public void setRemainTime(int remainTime) {
		this.remainTime = remainTime;
	}



	public LightGroupColor() {
		// TODO Auto-generated constructor stub
	}

}
