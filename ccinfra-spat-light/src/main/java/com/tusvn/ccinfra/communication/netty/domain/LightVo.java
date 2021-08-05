package com.tusvn.ccinfra.communication.netty.domain;

public class LightVo {
	
	private String msgType;
	
	public String getMsgType() {
		return msgType;
	}



	public void setMsgType(String msgType) {
		this.msgType = msgType;
	}
 


	private Object recvObj;
	
	private Long msgTime;
	public Long getMsgTime() {
		return msgTime;
	}



	public void setMsgTime(Long msgTime) {
		this.msgTime = msgTime;
	}



	public Long getRecvTime() {
		return recvTime;
	}



	public void setRecvTime(Long recvTime) {
		this.recvTime = recvTime;
	}



	public Long getFinTime() {
		return finTime;
	}



	public void setFinTime(Long finTime) {
		this.finTime = finTime;
	}



	private Long recvTime;
	private Long finTime;
	

	public Object getRecvObj() {
		return recvObj;
	}



	public void setRecvObj(Object recvObj) {
		this.recvObj = recvObj;
	}



	public LightVo() {
		// TODO Auto-generated constructor stub
	}

}
