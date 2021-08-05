package com.tusvn.ccinfra.communication.netty.model;
/**
 * 信号灯相位状态
 * @author tsing
 *
 */
public class PhaseState {

	/**信号灯颜色*/
	private int light;
	
	/**开始时间*/
	private int startTime;
	/**结束时间*/
	private int likelyEndTime;
	/**下一个持续时间*/
	private int nextDuration;
	
	
	//////////////////////////////////2021-4-20 liaoxb add 增加53---2020中spat结构
	
	private Long startUTCTime;    ///////开始时间 
	private Long likelyEndUTCTime;///////下一次结束时间
	
	private Long nextStartUTCTime; //////第2次开始时间
	private Long nextEndUTCTime;  ///////第2次结束时间
	
	//////////////////////////////////////////////
	
	public Long getStartUTCTime() {
		return startUTCTime;
	}

	public void setStartUTCTime(Long startUTCTime) {
		this.startUTCTime = startUTCTime;
	}

	public Long getLikelyEndUTCTime() {
		return likelyEndUTCTime;
	}

	public void setLikelyEndUTCTime(Long likelyEndUTCTime) {
		this.likelyEndUTCTime = likelyEndUTCTime;
	}

	public Long getNextStartUTCTime() {
		return nextStartUTCTime;
	}

	public void setNextStartUTCTime(Long nextStartUTCTime) {
		this.nextStartUTCTime = nextStartUTCTime;
	}

	public Long getNextEndUTCTime() {
		return nextEndUTCTime;
	}

	public void setNextEndUTCTime(Long nextEndUTCTime) {
		this.nextEndUTCTime = nextEndUTCTime;
	}

	
	
	
	public PhaseState(int light, int startTime, int likelyEndTime, int nextDuration) {
		super();
		this.light = light;
		this.startTime = startTime;
		this.likelyEndTime = likelyEndTime;
		this.nextDuration = nextDuration;
	}

	public PhaseState() {
		super();
	}

	public int getLight() {
		return light;
	}
	public void setLight(int light) {
		this.light = light;
	}
	
	public int getStartTime() {
		return startTime;
	}
	public void setStartTime(int startTime) {
		this.startTime = startTime;
	}
	public int getLikelyEndTime() {
		return likelyEndTime;
	}
	public void setLikelyEndTime(int likelyEndTime) {
		this.likelyEndTime = likelyEndTime;
	}

	public int getNextDuration() {
		return nextDuration;
	}
	public void setNextDuration(int nextDuration) {
		this.nextDuration = nextDuration;
	}
	
	
}
