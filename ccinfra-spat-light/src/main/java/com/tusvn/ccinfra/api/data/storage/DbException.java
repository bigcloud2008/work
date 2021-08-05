package com.tusvn.ccinfra.api.data.storage;

/****
 * @author liaoxiaobo
 * 
 * 
 * **/
public class DbException  extends Exception{
	
 
	private int errCode;
	private String errMsg;
	private Throwable ex;
	
	public DbException(String errMsg) {
		super(errMsg);
    }

	public DbException(int errCode, String errMsg, Throwable ex) {
		
		super(errMsg, ex);
		this.errCode=errCode;
 
	}
	
	public DbException(String errMsg, Throwable ex) {
		super(errMsg, ex);
	}
	
	public DbException(Throwable ex) {
		super(ex);
	}
 
}
