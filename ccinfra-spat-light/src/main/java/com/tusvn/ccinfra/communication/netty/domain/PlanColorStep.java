package com.tusvn.ccinfra.communication.netty.domain;


//////////色步时间序列信息
public class PlanColorStep {
	
	
	private int color;////红:3, 红闪2    黄7 黄闪8   绿 5 绿闪6
	
	private int total;
	
	public int getColor() {
		return color;
	}

	public void setColor(int color) {
		this.color = color;
	}

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	

	public PlanColorStep() {
		// TODO Auto-generated constructor stub
	}

}
