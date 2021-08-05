package com.tusvn.ccinfra.config.uitls;


import com.tusvn.ccinfra.config.ConfigProxy;

public class CfgUtils {
	
	public static String    kafka_server =ConfigProxy.getPropertyOrDefault("spat.kafka.bootstrap", "116.236.72.162:19092");
	
	public static String    topics       =ConfigProxy.getPropertyOrDefault("spat.receivetopic","topic10");  ////上海松鸿接收主题
	
	public static String    send_topics  =ConfigProxy.getPropertyOrDefault("spat.sendtopic","Rc_FUS_Data,Rc_PUB_Receive_SPAT"); ////
	
 
	public static String    groupId 	   ="spat_songhong_"+System.currentTimeMillis();
	
	public static Integer   pull_timeout  =ConfigProxy.getIntOrDefault("spat.pulltimeout", 1000);////1s
 
	
	public static Integer thread_num	= ConfigProxy.getIntOrDefault("spat.process.thread.num", 50);////20
	
	
	///////////////////////////////////////////////////////////////////////
	public static String  ROADS_CONVERTED     =ConfigProxy.getPropertyOrDefault("spad.roads",      "10:100");/////上海松鸿的路口id转为启迪路口id
	public static Integer PlatRegionId        =ConfigProxy.getIntOrDefault(     "spat.regionId",   3114);    /////上海
    
    
    
    
    ///////路口id对于经纬度，半径
    public static String ROAD_BASEINFO=ConfigProxy.getPropertyOrDefault("spat.roadInfo", "100,121.171355,31.279049,1000");///多个用;隔开
    
    ///////路口id_原始相位id0：目标相位id01,目标相位id02,... ,原始相位id1：目标相位id11,目标相位id12,..
    public static String PHASEIDS=ConfigProxy.getPropertyOrDefault("spat.phase.convert", "100_2:1|1:2,3|5:4|3:5,6|4:7,8,11,12");////多个用;隔开

	public CfgUtils() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String [] args)
	{
 
	}

}
