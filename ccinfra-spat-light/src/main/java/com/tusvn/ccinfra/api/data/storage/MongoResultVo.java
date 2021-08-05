package com.tusvn.ccinfra.api.data.storage;

import java.util.List;
import java.util.Map;

public class MongoResultVo {
	
	private List<Map<String, Object>>  res;
	
	public List<Map<String, Object>> getRes() {
		return res;
	}

	public void setRes(List<Map<String, Object>> res) {
		this.res = res;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	private long total;

	public MongoResultVo() {
		// TODO Auto-generated constructor stub
	}
	
	public MongoResultVo(long total, List<Map<String, Object>>  res) {
		// TODO Auto-generated constructor stub
		this.total=total;
		this.res=res;
	}

}
