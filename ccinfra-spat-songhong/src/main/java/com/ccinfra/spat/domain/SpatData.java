package com.ccinfra.spat.domain;

import java.util.List;
import java.util.UUID;

/**
 * 信号灯
 * @author tsing
 *
 */
public class SpatData {
	
	/**消息类型,固定值 "spat"*/
	private String  msgType = "spat";

	/**计数器编号,暂时传递定值127*/
	private int msgCnt = 127;
	
	/**唯一id*/
	private String uuid;
	
	
	/**平台发出该帧数据的时间戳*/
	private long timestamp;
	/**路口*/
	private List<Intersection> intersections;
	
	private List<Circle> targetCell;
	
	//private List<String> targetRsuIds = Arrays.asList("R-JY00B7");

	
	
	public SpatData() {
		super();
	}

	public SpatData(long timestamp, List<Intersection> intersections, List<Circle> targetCell) {
		super();
		this.timestamp = timestamp;
		this.intersections = intersections;
		this.targetCell = targetCell;
		this.uuid = UUID.randomUUID().toString();
	}
	
	public int getMsgCnt() {
		return msgCnt;
	}
	public void setMsgCnt(int msgCnt) {
		this.msgCnt = msgCnt;
	}
	public String getMsgType() {
		return msgType;
	}
	public void setMsgType(String msgType) {
		this.msgType = msgType;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public List<Intersection> getIntersections() {
		return intersections;
	}
	public void setIntersections(List<Intersection> intersections) {
		this.intersections = intersections;
	}
	public List<Circle> getTargetCell() {
		return targetCell;
	}
	public void setTargetCell(List<Circle> targetCell) {
		this.targetCell = targetCell;
	}
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

//	public List<String> getTargetRsuIds() {
//		return targetRsuIds;
//	}
//
//	public void setTargetRsuIds(List<String> targetRsuIds) {
//		this.targetRsuIds = targetRsuIds;
//	}
	
}
