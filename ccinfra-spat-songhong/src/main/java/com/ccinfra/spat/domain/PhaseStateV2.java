package com.ccinfra.spat.domain;

/**
 * 上通二期信号灯接口实体类
 * @author tsing
 *
 */
public class PhaseStateV2 {

	/**相位类别*/
	private Integer phaseType;
	/**周期总时长*/
	private Integer cycleTime;
	/**信号灯状态*/
	private Integer lightState;
	/**状态时长*/
	private Integer phaseTime;
	/**剩余时间*/
	private Integer lastTime;
	
	public PhaseStateV2() {
		super();
	}
	
	public PhaseStateV2(Integer phaseType, Integer cycleTime, Integer lightState, Integer phaseTime, Integer lastTime) {
		super();
		this.phaseType = phaseType;
		this.cycleTime = cycleTime;
		this.lightState = lightState;
		this.phaseTime = phaseTime;
		this.lastTime = lastTime;
	}

	public Integer getPhaseType() {
		return phaseType;
	}
	public void setPhaseType(Integer phaseType) {
		this.phaseType = phaseType;
	}
	public Integer getCycleTime() {
		return cycleTime;
	}
	public void setCycleTime(Integer cycleTime) {
		this.cycleTime = cycleTime;
	}
	public Integer getLightState() {
		return lightState;
	}
	public void setLightState(Integer lightState) {
		this.lightState = lightState;
	}
	public Integer getPhaseTime() {
		return phaseTime;
	}
	public void setPhaseTime(Integer phaseTime) {
		this.phaseTime = phaseTime;
	}
	public Integer getLastTime() {
		return lastTime;
	}
	public void setLastTime(Integer lastTime) {
		this.lastTime = lastTime;
	}
	
}
