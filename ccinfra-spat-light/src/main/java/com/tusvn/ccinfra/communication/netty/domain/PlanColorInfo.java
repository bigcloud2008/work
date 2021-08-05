package com.tusvn.ccinfra.communication.netty.domain;

import java.util.List;


//////----灯色信息
public class PlanColorInfo {
	
	private int groupNo;                     ////灯组编号
	
	private int groupType;                   ////灯组类型 
	private int step;                        ////色步数
	private List<PlanColorStep> colorSteps;  ////色步时间序列
	
	
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

	public int getStep() {
		return step;
	}

	public void setStep(int step) {
		this.step = step;
	}

	public List<PlanColorStep> getColorSteps() {
		return colorSteps;
	}

	public void setColorSteps(List<PlanColorStep> colorSteps) {
		this.colorSteps = colorSteps;
	}


 
	public PlanColorInfo() {
		// TODO Auto-generated constructor stub
	}
	
	

}
