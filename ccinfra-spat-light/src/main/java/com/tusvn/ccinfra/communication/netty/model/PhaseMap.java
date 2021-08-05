package com.tusvn.ccinfra.communication.netty.model;

public class PhaseMap {

	private int roadId;
	private int srcPhaseId;
	private int destPhaseId;
	private String note;
	private int green;
	private int greenblink;
	private int yellow;
	private int yellowblink;
	private int red;
	private int redblink;
	public int getRoadId() {
		return roadId;
	}
	public void setRoadId(int roadId) {
		this.roadId = roadId;
	}
	public int getSrcPhaseId() {
		return srcPhaseId;
	}
	public void setSrcPhaseId(int srcPhaseId) {
		this.srcPhaseId = srcPhaseId;
	}
	public int getDestPhaseId() {
		return destPhaseId;
	}
	public void setDestPhaseId(int destPhaseId) {
		this.destPhaseId = destPhaseId;
	}
	public String getNote() {
		return note;
	}
	public void setNote(String note) {
		this.note = note;
	}
	public int getGreen() {
		return green;
	}
	public void setGreen(int green) {
		this.green = green;
	}
	public int getGreenblink() {
		return greenblink;
	}
	public void setGreenblink(int greenblink) {
		this.greenblink = greenblink;
	}
	public int getYellow() {
		return yellow;
	}
	public void setYellow(int yellow) {
		this.yellow = yellow;
	}
	public int getYellowblink() {
		return yellowblink;
	}
	public void setYellowblink(int yellowblink) {
		this.yellowblink = yellowblink;
	}
	public int getRed() {
		return red;
	}
	public void setRed(int red) {
		this.red = red;
	}
	public int getRedblink() {
		return redblink;
	}
	public void setRedblink(int redblink) {
		this.redblink = redblink;
	}
}
