package com.ccinfra.spat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ccinfra.spat.service.SpatService;


/*****
 * 2021-8-2 liaoxb add
 * 
 * 上海松鸿10号路口接入
 * 
 * 
 * 零束项目新增10号路口（墨玉南路安礼路路口）信号灯数据
 * 
 * kafka broker: 116.236.72.162:19092
   topic: topic10
 * 
 * 
 * **/
public class SpatMain {
	
	private static final Logger logger = LogManager.getLogger(SpatMain.class);

	public SpatMain() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		
		logger.info("=====>SpatMain start");
		
		Thread t=new Thread(new SpatService());
		
		t.start();
		
		// TODO Auto-generated method stub

	}

}
