package com.tusvn.ccinfra.communication.netty.service;


import java.util.concurrent.ConcurrentHashMap;



public class LoginInfo {
	
	//////////////////当前信号方案色步信息
	////最外层key: roadId
	/////////最里层key:   进口方向:灯组编号:灯组类型:灯色  ====>时间
	////public volatile static ConcurrentHashMap<String, LinkedHashMap<String, Integer>>  map_currPlans  =new ConcurrentHashMap<>();
	
	
	/////------map_currPlans--->路口id:inDirection:groupType:color:groupNo====>totalTime
	public volatile static ConcurrentHashMap<String, Integer>  map_currPlans  =new ConcurrentHashMap<>();
	
	
	//////////////////////--------路口id:inDirection:groupType:color ====> totalTime
    //////public volatile static ConcurrentHashMap<String, Integer>  map_currPlansWithNoGroupNo  =new ConcurrentHashMap<>();
	
	
	public volatile static ConcurrentHashMap<Integer, ConcurrentHashMap<String, Integer>>  map_currPlansWithNoGroupNo  =new ConcurrentHashMap<>();
	
	
	/////////////////下一个周期信号方案色步信息
	///////public volatile static ConcurrentHashMap<String, LinkedHashMap<String, Integer>>  map_nextPlans  =new ConcurrentHashMap<>();
	public volatile static ConcurrentHashMap<String, Integer>  map_nextPlans  =new ConcurrentHashMap<>();
	
	
	
	//////////////////////--------路口id:inDirection:groupType:color ====> totalTime
	//////public volatile static ConcurrentHashMap<String, Integer>  map_nextPlansWithNoGroupNo  =new ConcurrentHashMap<>();
	
	public volatile static ConcurrentHashMap<Integer, ConcurrentHashMap<String, Integer>>  map_nextPlansWithNoGroupNo  =new ConcurrentHashMap<>();
	
	

	public LoginInfo() {
		// TODO Auto-generated constructor stub
	}

}
