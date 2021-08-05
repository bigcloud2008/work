package com.ccinfra.spat.domain;

public class SpatDataInfo {
	
	private SpatData spData;
	
	public SpatData getSpData() {
		return spData;
	}

	public void setSpData(SpatData spData) {
		this.spData = spData;
	}

	public Integer getRegionId() {
		return regionId;
	}

	public void setRegionId(Integer regionId) {
		this.regionId = regionId;
	}

	public Integer getCrossId() {
		return crossId;
	}

	public void setCrossId(Integer crossId) {
		this.crossId = crossId;
	}

	private Integer regionId;
	private Integer crossId;

	public SpatDataInfo() {
		// TODO Auto-generated constructor stub
	}

}
