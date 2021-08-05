package com.ccinfra.spat.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ccinfra.spat.domain.Center;
import com.ccinfra.spat.domain.Circle;
import com.ccinfra.spat.domain.CircleBean;
import com.ccinfra.spat.domain.Intersection;
import com.ccinfra.spat.domain.Phase;
import com.ccinfra.spat.domain.PhaseState;
import com.ccinfra.spat.domain.SpatData;
import com.ccinfra.spat.domain.SpatDataInfo;
import com.tusvn.ccinfra.api.data.storage.DBUtil;
import com.tusvn.ccinfra.api.data.storage.RedisUtils;
import com.tusvn.ccinfra.api.utils.KafkaUtils;
import com.tusvn.ccinfra.config.ConfigProxy;
import com.tusvn.ccinfra.config.uitls.CfgUtils;
 


public class SpatConvertService {
	
	
	private static final Logger logger = LogManager.getLogger(SpatConvertService.class);
	

	public static ConcurrentHashMap<String, AtomicInteger>   mapAi=new  ConcurrentHashMap<String, AtomicInteger>();
	
	
	public static ConcurrentHashMap<String, Circle>    intersectionMap = new ConcurrentHashMap<>();  /////路口id-----》经度，纬度，半径
	
	/////////////////////////////////////
	public static ConcurrentHashMap<String, Integer>   mapPhases       = new ConcurrentHashMap<>();  /////路口id:进口方向:灯组编号:灯组类型 ===>相位id
	
	public static ConcurrentHashMap<String, Integer>   mapColors       = new ConcurrentHashMap<>();  /////路口id:进口方向:灯组编号:灯组类型:原始灯色 ===>配时时间
	////////////////////////////////////
    
	
	public static  ConcurrentHashMap<Integer, Integer> ROADID_MAPS=new ConcurrentHashMap<>();  ////10:100,2:12
    
    
    public static ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, String[]>>   mapPhasesConverted       = new ConcurrentHashMap<>();  /////原始相位id转为启迪相位id
 
	
	////light转换
	////phaseId转换
    
    
    public static void init()
    {
    	/////////////////------------对方10号路口转换为我们地图中路口id，多个用;隔开
		String [] arr_roads=CfgUtils.ROADS_CONVERTED.split("\\;");
		for(String line_road: arr_roads)
		{
			String [] roads=line_road.split("\\:");
			ROADID_MAPS.put(Integer.parseInt(roads[0]), Integer.parseInt(roads[1]));
		}
		
		//////readDb();
		try
		{
			String[] line_road_baseinfos=CfgUtils.ROAD_BASEINFO.split("\\;");
			
			for(String tmp_road_baseinfo: line_road_baseinfos)
			{
				
				String [] arr_road_baseinfos=tmp_road_baseinfo.split("\\,");
				
				Center center=new Center();
				center.setLongitude( Double.parseDouble(arr_road_baseinfos[1]) );
				center.setLatitude( Double.parseDouble(arr_road_baseinfos[2]) );
				
				Double radius=Double.parseDouble(arr_road_baseinfos[3]);
	 
				
				CircleBean circleBean=new CircleBean(center, radius);
				
				Circle circle=new Circle(circleBean);
				
				intersectionMap.put(arr_road_baseinfos[0]+"_circle", circle);
				
			}
			
			////////////////////////////////////////////////////////////
			
			String [] arr_line_phaseids=CfgUtils.PHASEIDS.split("\\;");
			
			for(String tmp_line_phaseid: arr_line_phaseids)
			{
				/////100_2:1|1:2,3|4:11,12|5:4|3:5,6|4:7,8
				
				
				ConcurrentHashMap<Integer, String[]> mapRealPhasesConverted=new ConcurrentHashMap<Integer, String[]>();
				
				String[] arr_phaseids1=tmp_line_phaseid.split("\\_");
				
				Integer tmp_crossId=Integer.parseInt(arr_phaseids1[0]);
				
				String  tmp_part2  =arr_phaseids1[1];          //////2:1|1:2,3|4:11,12|5:4|3:5,6|4:7,8
				
				String[] arr_phaseids=tmp_part2.split("\\|");
				
				for(String tmp_phaseId: arr_phaseids)
				{
					String[] tmp_line=tmp_phaseId.split("\\:"); /////1:2,3
					
					Integer  src_phaseId  =Integer.parseInt(tmp_line[0]);
					
					String[] dest_phaseids=tmp_line[1].split("\\,");
					
					mapRealPhasesConverted.put(src_phaseId, dest_phaseids);
				}
				
				
				mapPhasesConverted.put(tmp_crossId, mapRealPhasesConverted);
				
			}
 
			
			
		}
		catch(Exception ex)
		{
			logger.error("in convert ROAD_BASEINFO", ex);
		}
    }
	
	
	static {
		
		init();
		
	}
	
	////--------------暂时不使用
	public static void readDb()
	{
		
		try
		{
			//////cross_id, longitude, latitude,  radius
			List<Map<String, Object>> machineInfo = DBUtil.selectList("select * from tb_signal_machine_info");
			
			if(machineInfo!=null && machineInfo.size()>0)
			for (Map<String, Object> map : machineInfo) {
				String crossId =    ""+ map.get("cross_id") ;

				Circle circle=null;
				
				
				Center center=new Center();
				center.setLongitude((Double)map.get("longitude"));
				center.setLatitude( (Double)map.get("latitude"));
				
				Double radius=(Double)map.get("radius");
				
				if(radius==null)
					radius=1000D;
				
				CircleBean circleBean=new CircleBean(center, radius);
				
				circle=new Circle(circleBean);
				
				intersectionMap.put(crossId+"_circle", circle);
				
				
				
			}
			
			/////////////////////////////路口相位转换表
			List<Map<String, Object>> map_phases = DBUtil.selectList("select * from t_spat_phase");
			
			if(map_phases!=null && map_phases.size()>0)
			for (Map<String, Object> map : map_phases) {
				String  crossId 			= (String) map.get("road_id");
				Integer phaseId 			= (Integer) map.get("phase_id");
				Integer entranceDirection 	= (Integer) map.get("entrance_direction");
				///////Integer groupNo 			= (Integer) map.get("group_no");
				Integer groupType 			= (Integer) map.get("group_type");
				Integer isNeed 				= (Integer) map.get("is_need");
 
				///////String tmp_key=crossId+":"+entranceDirection+":"+groupNo+":"+groupType;
				
				//////String tmp_key=crossId+":"+entranceDirection+":"+groupType;
				
				//////方向的角度：灯组类型
				String tmp_key=entranceDirection+":"+groupType;
				
				mapPhases.put(tmp_key, phaseId);
			}
			
//			/////////////////////////////路口各方向灯色配时时间表---每个相位下3个灯，红，黄，绿
//			List<Map<String, Object>> map_colors = DBUtil.selectList("select * from t_spat_color");
//			
//			if(map_colors!=null && map_colors.size()>0)
//			for (Map<String, Object> map : map_colors) {
//				String  crossId 			= (String) map.get("road_id");
//				Integer entranceDirection 	= (Integer) map.get("entrance_direction");
//				/////Integer groupNo 			= (Integer) map.get("group_no");
//				Integer groupType 			= (Integer) map.get("group_type");
//				Integer color 				= (Integer) map.get("color");  
//				Integer total 				= (Integer) map.get("total");  ///////----配时时间
// 
//				//////String tmp_key=crossId+":"+entranceDirection+":"+groupNo+":"+groupType+":"+color;
// 
//				String tmp_key=crossId+":"+entranceDirection+":"+groupType+":"+color;
//				
//				mapColors.put(tmp_key, total);
//			}
			
			logger.info("=====>intersectionMap="+intersectionMap);
			logger.info("==============>[MAPPHASES]=====>mapPhases="+mapPhases);
			logger.info("=====>mapColors="+mapColors);
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			logger.error("in readDb:"+ex.getMessage(), ex);
		}
		
	}
	
	
	public SpatConvertService() {
		// TODO Auto-generated constructor stub
	}
	
	/****
	 * 将松鸿的spat转为启迪的spat格式
	 * 
	 * 
	 * **/
	public static SpatDataInfo convertSpatData(String json)
	{
		SpatDataInfo spDataInfo=new SpatDataInfo();
		
		
		SpatData spData=null;
		
		
		Integer crossId       =0;
		
		try
		{
			JSONObject jsonObj=JSON.parseObject(json);
			
			Integer areaId        =jsonObj.getInteger("areaId");
			
			Integer intersectionId=jsonObj.getInteger("intersectionId");////原始路口id
			
			crossId               =ROADID_MAPS.get(intersectionId);/////////转换后的路口id
			
			
			AtomicInteger ai=mapAi.get(""+crossId);
			if(ai==null)
				ai=new AtomicInteger(0);
			
			
			int cnt=ai.incrementAndGet();
			
			if(cnt>=127)
			{
				ai.set(0);;
				cnt=0;
			}
			
			
			
			List<Circle> targetCell=new ArrayList<>();
			
			Circle circle=intersectionMap.get(crossId+"_circle");
			if(circle!=null)
				targetCell.add(circle);

			List<Intersection> intersections=new ArrayList<>();
			
			///////////---------------------------------------------------主逻辑
			List<Phase> phases=new ArrayList<>();
			
			JSONArray src_phases=jsonObj.getJSONArray("phases");
			for(Object o_phase: src_phases)
			{
				JSONObject j_phase          =(JSONObject)o_phase;
				
				Integer    src_phaseId      =j_phase.getInteger("phaseId"); /////////--------------上海松鸿的相位id，要转为启迪的相位id
				
				JSONArray  src_phaseStatuses=j_phase.getJSONArray("phaseStatuses");
				
				
				List<PhaseState> phaseStates=new ArrayList<>();
				
				for(Object o_phaseStatuse: src_phaseStatuses)
				{
					JSONObject phaseStatuse=(JSONObject)o_phaseStatuse;
					
					Integer lightState   =phaseStatuse.getInteger("lightState");  //////1-绿灯，2-黄灯，3-红灯
					
					Integer light=0;
					
					if(lightState==1)
						light=5;
					else if(lightState==2)
						light=7;
					else if(lightState==3)
						light=3;
					
					
					Integer likelyEndTime=phaseStatuse.getInteger("likelyEndTime");
					Integer nextDuration =phaseStatuse.getInteger("nextDuration");
					Integer startTime    =phaseStatuse.getInteger("startTime");
					
					////(int light, int startTime, int likelyEndTime, int nextDuration)
					PhaseState phaseState=new PhaseState(light, startTime, likelyEndTime, nextDuration);
					
					phaseStates.add(phaseState);
					
				}
				
				////////////////////////////////////////////////////
				ConcurrentHashMap<Integer, String[]> converted_phaseIds=mapPhasesConverted.get(crossId);
				
				if(converted_phaseIds==null)
				{
					logger.error("crossId="+crossId+ " can not find converted phase map!" );
					return null;
				}
				
				String [] arr_dest_phaseId=converted_phaseIds.get(src_phaseId);
				if(arr_dest_phaseId==null || arr_dest_phaseId.length==0)
				{
					logger.error("crossId="+crossId+ "'s src_phaseId="+src_phaseId+" can not find dest phaseId!" );
					return null;
				}
				
				for(String tmp_dest_phaseId: arr_dest_phaseId)
				{
					////////(int phaseId, List<PhaseState> phaseStates)
					Phase phase=new Phase(Integer.parseInt(tmp_dest_phaseId), phaseStates);
					
					phases.add(phase);
				}
				
				
				
			}
			
			/////////////////////////////////////////////////////////////////////////
			
			if(phases==null)
				return null;
			
			long intersectionTimestamp=System.currentTimeMillis();
			
//			logger.info("====>crossId="+crossId);
//			logger.info("====>regionId="+PlatRegionId);
//			logger.info("====>converted_regionId="+CfgUtils.PlatRegionId);
//			logger.info("====>intersectionTimestamp="+intersectionTimestamp);
//			logger.info("====>phases="+JSON.toJSONString(phases));
			
	 
			
			Intersection intersection=new Intersection(crossId, CfgUtils.PlatRegionId, intersectionTimestamp, phases); /////regionId
			
			intersections.add(intersection);
			
			long timestamp=System.currentTimeMillis();
			spData=new SpatData(timestamp, intersections, targetCell);
			spData.setMsgCnt(cnt);
			
 
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			logger.error("in convertSpatData", ex);
		}
		
		spDataInfo.setRegionId(CfgUtils.PlatRegionId);
		spDataInfo.setCrossId(crossId);
		spDataInfo.setSpData(spData);
		
		
		return spDataInfo;
	}
	
	
	///////--------------3个时间乘以10
	public static SpatData transferTime(SpatData spData)
	{
		List<Intersection> intersections=spData.getIntersections();
		
		for(Intersection intersection: intersections)
		{
			List<Phase> phases=intersection.getPhases();
			
			for(Phase phase: phases)
			{
				List<PhaseState> phaseStates=phase.getPhaseStates();
				
				for(PhaseState phaseState:phaseStates)
				{
					phaseState.setStartTime(phaseState.getStartTime()*10);
					phaseState.setLikelyEndTime(phaseState.getLikelyEndTime()*10);
					phaseState.setNextDuration(phaseState.getNextDuration()*10);
				}
				
			}
			
		}
		
		return spData;
		
	}
	

	
	public static JSONObject convertSpat2Json(Integer regionId, SpatData spData)
	{
		JSONObject jsonObject = new JSONObject();
 
		JSONObject data=(JSONObject)JSONObject.toJSON(spData);  ///java对象转JSONObject
		
		JSONObject messgae = new JSONObject();
		messgae.put("data", 	Arrays.asList(data));
		messgae.put("regionId", regionId);
		
		
		jsonObject.put("sendDataType", "STMS_SPAT");
		jsonObject.put("message",      Arrays.asList(messgae));
		
		return jsonObject;
		
	}
	
	public static void writeKafka(Integer roadId, Integer regionId, SpatData spData)
	{
		long c0=System.currentTimeMillis();
		JSONObject jsonObject=convertSpat2Json( regionId, spData); //////////转为正式下发格式
		long c1=System.currentTimeMillis();
		logger.info("===============>a1111111, usetime c1-c0="+(c1-c0)+" ms"  );
		
		
		writeKafka(  ""+roadId,  jsonObject);
		long c2=System.currentTimeMillis();
		logger.info("===============>a1111111, usetime c2-c1="+(c2-c1)+" ms"  );
	}
	
	public static void writeKafka(String key, JSONObject jsonObject) {
		String value = JSON.toJSONString(jsonObject, SerializerFeature.DisableCircularReferenceDetect);
		
//		if(ConfigProxy.getBooleanOrDefault("printSpatDataDebugLog", false)){
//			logger.info("Send to kafka value:{}",value);
//		}
		////List<String> topics = ConfigProxy.getList("send.spat.topics");  ////FUS_Data,PUB_Receive_SPAT
		
		String stopics = ConfigProxy.getPropertyOrDefault("send.spat.topics", "FUS_Data,PUB_Receive_SPAT");
		String [] topics=stopics.split("\\,");
		
	    for (String topicName : topics) {
			String topic = ConfigProxy.getProperty("topic.prefix")+topicName;
			
			long d0=System.currentTimeMillis();
			KafkaUtils.getInstance().send(topic, key, value);
			long d1=System.currentTimeMillis();
			
			logger.info("===============>a1111111, topicName="+topic+" usetime d1-d0="+(d1-d0)+" ms"  );
			
			
		}
	    
	}
	
	
	public static void writeRedis(SpatData spData)
	{
		List<Intersection> intersections=spData.getIntersections();
		
		for(Intersection intersection: intersections)
		{
			Integer nodeId					=intersection.getNodeId();
			Long    intersectionTimestamp	=intersection.getIntersectionTimestamp();
			
			List<Phase> phases=intersection.getPhases();
			
			for(Phase phase: phases)
			{
				List<PhaseState> phaseStates=phase.getPhaseStates();
				
				
				String key = "SPAT:GLOSA:"+nodeId+":"+phase.getPhaseId();
				
				
				
				String json=JSON.toJSONString(phaseStates);
				
				////////logger.info("write redis: key="+key+", json="+json);
				
				JSONArray j_phaseStates=JSONArray.parseArray(json);
				
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("intersectionTimestamp", intersectionTimestamp);
				jsonObject.put("phaseStates", 			j_phaseStates);
				
				
				int expireSeconds=ConfigProxy.getIntOrDefault("spat.glosa.expirTimeInSecond", 120);
				RedisUtils.Strings.setWithTTL(key, jsonObject.toJSONString(), expireSeconds);
 
				
				///////////RedisUtils.Strings.set(key, jsonObject.toJSONString());
				
				/////////////String tmp_val=RedisUtils.Strings.get(key);
				/////////////logger.info("read redis: key="+key+", json="+json);
				
				
				///////////
				//////////RedisUtils.Keys.expired(key, ConfigProxy.getIntOrDefault("spat.glosa.expirTimeInSecond", 120));
				
			}
			
		}
		
	}
	
	public static void main(String [] args)
	{
		init();
		
		////
		System.out.println("ROADID_MAPS="+JSON.toJSONString(ROADID_MAPS));
		
		System.out.println("intersectionMap="+JSON.toJSONString(intersectionMap));
		
		System.out.println("mapPhasesConverted="+JSON.toJSONString(mapPhasesConverted));
		
	}
	
}
