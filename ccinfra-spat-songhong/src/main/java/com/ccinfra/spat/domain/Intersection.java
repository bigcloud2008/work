package com.ccinfra.spat.domain;
/**
 * 路口
 * @author tsing
 *
 */

import java.util.List;

public class Intersection {
	
	/**信号灯作用node所在区域唯一编号*/
	private int regionId = 31000;
	/**信号灯作用node在该区域内唯一编号*/
	private int nodeId = 7;
	/**路口信号机工作状态标志,固定值32*/
	private int status = 32;
	/**从信号机获得该字段信息的系统时间戳，精确到毫秒*/
	private long intersectionTimestamp;
	/**相位*/
	private List<Phase> phases;
	
	public Intersection() {
		super();
	}

	public Intersection(int nodeId) {
		super();
		this.nodeId = nodeId;
	}
	public Intersection(int nodeId, int regionId, long intersectionTimestamp, List<Phase> phases) {
		super();
		this.nodeId = nodeId;
		this.regionId = regionId;
		this.intersectionTimestamp = intersectionTimestamp;
		this.phases = phases;
	}

	public int getRegionId() {
		return regionId;
	}
	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}
	public int getNodeId() {
		return nodeId;
	}
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public long getIntersectionTimestamp() {
		return intersectionTimestamp;
	}
	public void setIntersectionTimestamp(long intersectionTimestamp) {
		this.intersectionTimestamp = intersectionTimestamp;
	}
	public List<Phase> getPhases() {
		return phases;
	}
	public void setPhases(List<Phase> phases) {
		this.phases = phases;
	}
	
	
}
