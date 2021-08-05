package com.tusvn.ccinfra.config.uitls;

import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSON;
import com.tusvn.ccinfra.config.ConfigProxy;

public class CfgUtils {
	
	public static final String  ROADS   =ConfigProxy.getPropertyOrDefault("spat.roads", "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,23,24,25,26,27,28");/////要处理的28个路口id
	
	public static Integer PlatRegionId=ConfigProxy.getIntOrDefault("spat.regionId",   1101);/////110115
	
	
	public static final Integer SERVER_PORT=ConfigProxy.getIntOrDefault("spat.server.port", 50001);
	
	
	public static ConcurrentHashMap<String, String> map_crossId=new ConcurrentHashMap<>();
	
	static
	{
		String [] arr=ROADS.split("\\,");
		
		
		if(arr!=null && arr.length>0)
		{
			for(String roadId: arr)
			{
				map_crossId.put(roadId, roadId);
				
			}
			
		}
		
	}

	public CfgUtils() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String [] args)
	{
		System.out.println("=================>map_crossId="+JSON.toJSONString(map_crossId));
		
	}

}
