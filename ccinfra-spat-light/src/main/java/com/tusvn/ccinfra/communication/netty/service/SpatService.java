package com.tusvn.ccinfra.communication.netty.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import com.tusvn.ccinfra.api.data.storage.DBUtil;
import com.tusvn.ccinfra.api.data.storage.RedisUtils;
import com.tusvn.ccinfra.api.data.storage.RedisUtilsForTest;
import com.tusvn.ccinfra.api.utils.KafkaUtils;
import com.tusvn.ccinfra.config.ConfigProxy;
import com.tusvn.ccinfra.config.uitls.CfgUtils;
import com.tusvn.ccinfra.config.uitls.SpatConvertUtils;
import com.tusvn.ccinfra.communication.netty.domain.EntranceColor;
import com.tusvn.ccinfra.communication.netty.domain.EntranceLightGroup;
import com.tusvn.ccinfra.communication.netty.domain.LightGroupColor;
import com.tusvn.ccinfra.communication.netty.domain.Plan;
import com.tusvn.ccinfra.communication.netty.handler.SpatHandler;
import com.tusvn.ccinfra.communication.netty.main.CarCloudServer;
import com.tusvn.ccinfra.communication.netty.model.Center;
import com.tusvn.ccinfra.communication.netty.model.Circle;
import com.tusvn.ccinfra.communication.netty.model.CircleBean;
import com.tusvn.ccinfra.communication.netty.model.Intersection;
import com.tusvn.ccinfra.communication.netty.model.Phase;
import com.tusvn.ccinfra.communication.netty.model.PhaseState;
import com.tusvn.ccinfra.communication.netty.model.SpatData;

public class SpatService {
	private static final Logger logger = LogManager.getLogger(SpatService.class);
	
	
	////////public static AtomicInteger ai=new AtomicInteger(0);
	
	
	public static ConcurrentHashMap<String, AtomicInteger>   mapAi=new  ConcurrentHashMap<String, AtomicInteger>();
	
	
	public static ConcurrentHashMap<String, Circle>    intersectionMap = new ConcurrentHashMap<>();  /////路口id-----》经度，纬度，半径
	
	public static ConcurrentHashMap<String, Integer>   mapPhases       = new ConcurrentHashMap<>();  /////路口id:进口方向:灯组编号:灯组类型 ===>相位id
	
	public static ConcurrentHashMap<String, Integer>   mapColors       = new ConcurrentHashMap<>();  /////路口id:进口方向:灯组编号:灯组类型:原始灯色 ===>配时时间
	
	//////public static Integer PlatRegionId=ConfigProxy.getIntOrDefault("spat.regionId",   1101);/////110115
	
	////light转换
	////phaseId转换
	
	
	static {
		
		readDb();

		
	}
	
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
			/////////////////////////////路口各方向灯色配时时间表---每个相位下3个灯，红，黄，绿
			List<Map<String, Object>> map_colors = DBUtil.selectList("select * from t_spat_color");
			
			if(map_colors!=null && map_colors.size()>0)
			for (Map<String, Object> map : map_colors) {
				String  crossId 			= (String) map.get("road_id");
				Integer entranceDirection 	= (Integer) map.get("entrance_direction");
				/////Integer groupNo 			= (Integer) map.get("group_no");
				Integer groupType 			= (Integer) map.get("group_type");
				Integer color 				= (Integer) map.get("color");  
				Integer total 				= (Integer) map.get("total");  ///////----配时时间
 
				//////String tmp_key=crossId+":"+entranceDirection+":"+groupNo+":"+groupType+":"+color;
 
				String tmp_key=crossId+":"+entranceDirection+":"+groupType+":"+color;
				
				mapColors.put(tmp_key, total);
			}
			
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
	
	
	public SpatService() {
		// TODO Auto-generated constructor stub
	}
	
	
	/*******
	 * ConcurrentHashMap<String, LinkedHashMap<String, Integer>>  map_currPlans
	 * 
	 * key:String 路口id
	 * 
	 * value: LinkedHashMap<String, Integer>   进口方向:灯组编号:灯组类型:灯色  ====>配时时间
	 * 
	 * 
	 * 
	 * **/
	public static SpatData convertSpatData(EntranceColor colorVo, ConcurrentHashMap<String, Integer>  map_currPlansWithNoGroupNo)
	{
		SpatData spData=null;
		
		if(colorVo==null)
		{
			logger.error("=====>colorVo is null!");
			return null;
		}
 
		Integer regionId=colorVo.getRegionId();
		Integer crossId =colorVo.getRoadId();
		
		
		AtomicInteger ai=mapAi.get(""+crossId);
		if(ai==null)
			ai=new AtomicInteger(0);
		
		
		int cnt=ai.incrementAndGet();
		
		if(cnt>=127)
		{
			ai.set(0);;
			cnt=0;
		}
		
		mapAi.put(""+crossId, ai);
		
		
		List<Circle> targetCell=new ArrayList<>();
		
		Circle circle=intersectionMap.get(crossId+"_circle");
		if(circle!=null)
			targetCell.add(circle);
		else  ////////否则使用消息中的lon， lat
		{
			logger.error("crossId=["+crossId+"] can not find circle!");
			
			if(colorVo.getLon()!=null && colorVo.getLat()!=null)
			{
				Center center=new Center();
				center.setLongitude(colorVo.getLon());
				center.setLatitude( colorVo.getLat());
			
				CircleBean circleBean=new CircleBean(center, 1000);
			
				circle=new Circle(circleBean);
				
				targetCell.add(circle);
			}
			
		}
		
		List<Intersection> intersections=new ArrayList<>();
		
		
		///////////---------------------------------------------------主逻辑
		List<Phase> phases=convertPhases(  ""+crossId,   colorVo, map_currPlansWithNoGroupNo);////////////----------------important
		
		if(phases==null)
			return null;
		
		long intersectionTimestamp=System.currentTimeMillis();
		
		logger.info("====>crossId="+crossId);
		logger.info("====>regionId="+regionId);
		logger.info("====>converted_regionId="+CfgUtils.PlatRegionId);
		logger.info("====>intersectionTimestamp="+intersectionTimestamp);
		logger.info("====>phases="+JSON.toJSONString(phases));
		
 
		
		Intersection intersection=new Intersection(crossId, CfgUtils.PlatRegionId, intersectionTimestamp, phases); /////regionId
		
		intersections.add(intersection);
		
		long timestamp=System.currentTimeMillis();
		spData=new SpatData(timestamp, intersections, targetCell);
		spData.setMsgCnt(cnt);
		
		
		//////return transferTime(spData);
		
		return spData;
		
	}
	
//	public static SpatData convertSpatData(JSONObject jsonObj, ConcurrentHashMap<String, Integer>  map_currPlansWithNoGroupNo)
//	{
//		SpatData spData=null;
//		
//		if(jsonObj==null)
//			return null;
//		
//		
//		EntranceColor colorVo=JSONObject.toJavaObject(jsonObj, EntranceColor.class);
//		
//		Integer regionId=colorVo.getRegionId();
//		Integer crossId =colorVo.getRoadId();
//		
//		
//		
//		int cnt=ai.incrementAndGet();
//		
//		if(cnt>127)
//		{
//			ai.set(0);;
//			cnt=0;
//		}
//		
//		List<Circle> targetCell=new ArrayList<>();
//		
//		Circle circle=intersectionMap.get(crossId+"_circle");
//		if(circle!=null)
//			targetCell.add(circle);
//		else  ////////否则使用消息中的lon， lat
//		{
//			logger.error("crossId=["+crossId+"] can not find circle!");
//			
//			if(colorVo.getLon()!=null && colorVo.getLat()!=null)
//			{
//				Center center=new Center();
//				center.setLongitude(colorVo.getLon());
//				center.setLatitude( colorVo.getLat());
//			
//				CircleBean circleBean=new CircleBean(center, 1000);
//			
//				circle=new Circle(circleBean);
//				
//				targetCell.add(circle);
//			}
//			
//		}
//		
//		List<Intersection> intersections=new ArrayList<>();
//		
//		
//		///////////---------------------------------------------------主逻辑
//		List<Phase> phases=convertPhases(  ""+crossId,   colorVo, map_currPlansWithNoGroupNo);////////////----------------important
//		
//		if(phases==null)
//			return null;
//		
//		long intersectionTimestamp=System.currentTimeMillis();
//		
//		logger.info("====>crossId="+crossId);
//		logger.info("====>regionId="+regionId);
//		logger.info("====>intersectionTimestamp="+intersectionTimestamp);
//		logger.info("====>phases="+JSON.toJSONString(phases));
//		
// 
//		
//		Intersection intersection=new Intersection(crossId, regionId, intersectionTimestamp, phases);
//		
//		intersections.add(intersection);
//		
//		long timestamp=System.currentTimeMillis();
//		spData=new SpatData(timestamp, intersections, targetCell);
//		
//		
//		return transferTime(spData);
//		
//	}
//	
	
	
	public static List<Phase> convertPhases(String roadId, EntranceColor colorVo, ConcurrentHashMap<String, Integer>  map_currPlansWithNoGroupNo)
	{
		List<Phase> phases=new ArrayList<>();
		
		List<EntranceLightGroup> groups=colorVo.getGroups();
		
		for(EntranceLightGroup group: groups)
		{
			Integer entranceDirection=group.getEntranceDirection();////进口方向
			
			List<LightGroupColor> groupColors=group.getGroupColors();
			
			
			HashMap<String, Integer> map_dup=new HashMap<>();//////-----排除掉groupNo
			
			for(LightGroupColor groupColor: groupColors)
			{
				int groupNo    =groupColor.getGroupNo();  ////灯组编号--------对应车道 
				
				int groupType  =groupColor.getGroupType();////灯组类型
				
				if(groupType!=1 && groupType!=2 && groupType!=3 && groupType!=4  && groupType!=9)  //////----------------2021-06-07 liaoxb 1:直行 2： 左转 3：右转 4：机动车信号灯 8:人行横道灯  9：掉头 
					continue;
				
				
				
				int color      =groupColor.getColor();    ////2---红闪， 3----红   5--绿  6--绿闪   7---黄   8--黄闪
				
				int status     =groupColor.getStatus();   ////0---无灯， 1---灭灯  2---亮灯   3----闪烁 --------未使用
				
				Integer remainTime =groupColor.getRemainTime() & 0xff;  //////剩余时间
			
			
				String tmp_key=roadId+":"+entranceDirection+":"+groupType+":"+color;
				map_dup.put(tmp_key, remainTime);
			
			}
			
			logger.info("======roadId="+roadId+"xxxxxxxxxxxxxxxxx==>map_dup="+JSON.toJSONString(map_dup) );
			
			Long intersectionTimestamp=System.currentTimeMillis();
			
			if(map_dup!=null && map_dup.size()>0)
			{
				for(Map.Entry<String, Integer> entry: map_dup.entrySet())
				{
					String  tmp_key   =entry.getKey();
					Integer remainTime=entry.getValue();
					
					String [] arr_tmp_key=tmp_key.split("\\:");
					
					String  s_roadId			=arr_tmp_key[0];
					String  s_entranceDirection	=arr_tmp_key[1];
					String  s_groupType			=arr_tmp_key[2];
					Integer color				=Integer.parseInt(arr_tmp_key[3]);
					
					

					
					
					/////tmp_key=17:90:4:6, remainTime=20
					logger.info("xxxxxxxxxroadId="+roadId+"xxxxxxxxxxxxxxxx==>tmp_key="+tmp_key+", remainTime="+remainTime );
					
					
					///////////////////////////////////////
					/////crossId+":"+entranceDirection+":"+groupType;
					/////////String  tmp_phase_key     =roadId+":"+entranceDirection+":"+s_groupType;
					String  tmp_phase_key     =entranceDirection+":"+s_groupType;

					logger.info("====roadId="+roadId+"=====>tmp_phase_key="+tmp_phase_key);
					
					Integer phaseId     =mapPhases.get(tmp_phase_key);  ////////根据方向的角度+灯组类型 转换为相位id
					
					if(phaseId==null)
					{
						
						logger.info("=roadId="+roadId+"==>[NOPHASEID]can not find phaseId, tmp_phase_key="+tmp_phase_key+", mapPhases="+JSON.toJSONString(mapPhases));
						
						continue;
					}
					
					logger.info("=roadId="+roadId+"=======>phaseId="+phaseId);
					
					
					if(color!=3 && color!=5 && color!=7)
					{
						logger.info("====[SKIPCOLOR]=========>roadId="+roadId+", phaseId="+phaseId+", color="+color+" is not in 3,5,7, skip this record!");
						continue;
					}
					
					
					
					///////////////////////////////////////
					
					/////--------路口id:inDirection:groupType:color ====> totalTime
					
					String green_key  =roadId+":"+entranceDirection+":"+s_groupType+":"+5;
					String yellow_key =roadId+":"+entranceDirection+":"+s_groupType+":"+7;
					String red_key    =roadId+":"+entranceDirection+":"+s_groupType+":"+3;
					
					logger.info("===roadId="+roadId+"======>green_key="+green_key);
					logger.info("===roadId="+roadId+"======>yellow_key="+yellow_key);
					logger.info("===roadId="+roadId+"======>red_key="+red_key);
					
					Integer nextDuration_green =map_currPlansWithNoGroupNo.get(roadId+":"+entranceDirection+":"+s_groupType+":"+5);////5:绿  6:绿闪
					Integer nextDuration_yellow=map_currPlansWithNoGroupNo.get(roadId+":"+entranceDirection+":"+s_groupType+":"+7);////7:黄  8:黄闪 
					Integer nextDuration_red   =map_currPlansWithNoGroupNo.get(roadId+":"+entranceDirection+":"+s_groupType+":"+3);////3:红  2:红闪
					
					
					if(nextDuration_green==null || nextDuration_yellow==null || nextDuration_red==null)
					{
						logger.error("==roadId="+roadId+"phaseId="+phaseId+"=>tmp_key="+tmp_key+",remainTime="+remainTime+",nextDuration_green or nextDuration_yellow or nextDuration_red is null:"+nextDuration_green+","+nextDuration_yellow+","+nextDuration_red  );
						continue ;
					}
					
					
					logger.info("==roadId="+roadId+"=======>nextDuration_green="+nextDuration_green);
					logger.info("==roadId="+roadId+"=======>nextDuration_yellow="+nextDuration_yellow);
					logger.info("==roadId="+roadId+"=======>nextDuration_red="+nextDuration_red);
					
					
					List<PhaseState> phaseStates=new ArrayList<>();
					
					///////////5绿---6绿闪----7黄---8黄闪----3红
					///////////5绿---7黄----3红
					if(color==5) { /////当前绿亮
//						PhaseState state_green=new PhaseState(5, 0, remainTime,  nextDuration_green);
//						phaseStates.add(state_green);
//						
//						PhaseState state_yellow=new PhaseState(7, remainTime, nextDuration_yellow,  nextDuration_yellow);
//						phaseStates.add(state_yellow);
//						
//						PhaseState state_red=new PhaseState(3, remainTime+nextDuration_yellow, nextDuration_red,  nextDuration_red);
//						phaseStates.add(state_red);
						
						
						HashMap<Integer, PhaseState> map_convert=SpatConvertUtils.calcLightTimeForCurrGreen(  intersectionTimestamp, remainTime, 
								nextDuration_green, nextDuration_yellow, nextDuration_red);
						
						
						PhaseState state_green =map_convert.get(5);
						phaseStates.add(state_green);
						
						PhaseState state_yellow=map_convert.get(7);
						phaseStates.add(state_yellow);
						
						PhaseState state_red   =map_convert.get(3);
						phaseStates.add(state_red);
						
						
						
					}
					else if(color==7) { /////当前黄亮
						
//						PhaseState state_green=new PhaseState(5, 0, remainTime+nextDuration_red,  nextDuration_green);
//						phaseStates.add(state_green);
//						
//						PhaseState state_yellow=new PhaseState(7, 0, remainTime,  nextDuration_yellow);
//						phaseStates.add(state_yellow);
//						
//						PhaseState state_red=new PhaseState(3, remainTime, nextDuration_red,  nextDuration_red);
//						phaseStates.add(state_red);
						
						
						HashMap<Integer, PhaseState> map_convert=SpatConvertUtils.calcLightTimeForCurrYellow(intersectionTimestamp, remainTime, 
								nextDuration_green, nextDuration_yellow, nextDuration_red);
						
						PhaseState state_green =map_convert.get(5);
						phaseStates.add(state_green);
						
						PhaseState state_yellow=map_convert.get(7);
						phaseStates.add(state_yellow);
						
						PhaseState state_red   =map_convert.get(3);
						phaseStates.add(state_red);
						
					}
					else if(color==3) { /////当前红亮
						
//						PhaseState state_green=new PhaseState(5, remainTime, nextDuration_green,  nextDuration_green);
//						phaseStates.add(state_green);
//						
//						PhaseState state_yellow=new PhaseState(7, remainTime+nextDuration_green, nextDuration_yellow,  nextDuration_yellow);
//						phaseStates.add(state_yellow);
//						
//						PhaseState state_red=new PhaseState(3, 0, remainTime,  nextDuration_red);
//						phaseStates.add(state_red);
						
						
						HashMap<Integer, PhaseState> map_convert=SpatConvertUtils.calcLightTimeForCurrRed(intersectionTimestamp, remainTime, 
								nextDuration_green, nextDuration_yellow, nextDuration_red);
						
						PhaseState state_green =map_convert.get(5);
						phaseStates.add(state_green);
						
						PhaseState state_yellow=map_convert.get(7);
						phaseStates.add(state_yellow);
						
						PhaseState state_red   =map_convert.get(3);
						phaseStates.add(state_red);
						
					}
					
					
					Phase phase =new Phase( phaseId,  phaseStates);
					phases.add(phase);
					
					
//					if(phaseId==4)
//					{
//						logger.info("===aaaaaaaaaaaaaaaaaaaaaaaaaaaa==>phases.size="+JSON.toJSONString(phases));
//					}
					
				}
				
				
			}
 	
		}
		
		return phases;
		
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
	
	
	public static void writeKafkaForOtherParty(String key, JSONObject jsonObject) {
		String value = JSON.toJSONString(jsonObject, SerializerFeature.DisableCircularReferenceDetect);
//		if(ConfigProxy.getBooleanOrDefault("printSpatDataDebugLog", false)){
//			logger.info("Send to kafka value:{}",value);
//		}
//		List<String> topics = ConfigProxy.getList("send.other.topics");  ////Pub_Spat_Light
//	    for (String string : topics) {
//			String topic = ConfigProxy.getProperty("topic.prefix")+string;/////Rc_Pub_Spat_Light
//			KafkaUtils.getInstance().send(topic, key, value);
//		}
		
		String topic = ConfigProxy.getPropertyOrDefault("send.notify.topics", "Rc_Pub_Spat_Light");
		KafkaUtils.getInstance().send(topic, key, value);
		
	}
	
	public static void writeKafka(Integer roadId, Integer regionId, SpatData spData)
	{
		long c0=System.currentTimeMillis();
		JSONObject jsonObject=convertSpat2Json( regionId, spData);
		long c1=System.currentTimeMillis();
		logger.info("===============>a1111111, usetime c1-c0="+(c1-c0)+" ms"  );
		
		
		writeKafka(  ""+roadId,  jsonObject);
		long c2=System.currentTimeMillis();
		logger.info("===============>a1111111, usetime c2-c1="+(c2-c1)+" ms"  );
	}
	
	
	public static void writeSchedulePlanToKafka(String key, JSONObject jsonObject) {
		String value = JSON.toJSONString(jsonObject, SerializerFeature.DisableCircularReferenceDetect);
//		if(ConfigProxy.getBooleanOrDefault("printSpatDataDebugLog", false)){
//			logger.info("Send to kafka value:{}",value);
//		}
		String topic = ConfigProxy.getPropertyOrDefault("send.scheduleplan.topics", "Rc_Pub_Receive_Spat_Schedule");
		KafkaUtils.getInstance().send(topic, key, value);
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
				
				
				//////////2021-7-30 liaoxb add 专用于测试
				String openWriteForTest=ConfigProxy.getPropertyOrDefault("enable.spat.writetotest", "yes");
				if(openWriteForTest!=null && openWriteForTest.equals("yes"))
					RedisUtilsForTest.Strings.setWithTTL(key, jsonObject.toJSONString(), expireSeconds);
				
				
				///////////RedisUtils.Strings.set(key, jsonObject.toJSONString());
				
				/////////////String tmp_val=RedisUtils.Strings.get(key);
				/////////////logger.info("read redis: key="+key+", json="+json);
				
				
				///////////
				//////////RedisUtils.Keys.expired(key, ConfigProxy.getIntOrDefault("spat.glosa.expirTimeInSecond", 120));
				
			}
			
		}
		
	}
	
	
	public static void test_color1()
	{
		String colorJson="{\"msgType\":\"color\",\"recvObj\":{\"alt\":0,\"entranceNum\":4,\"groups\":[{\"entranceDirection\":0,\"entranceGroupNum\":4,\"groupColors\":[{\"color\":5,\"groupNo\":1,\"groupType\":9,\"remainTime\":20,\"status\":2},{\"color\":3,\"groupNo\":2,\"groupType\":2,\"remainTime\":104,\"status\":2},{\"color\":3,\"groupNo\":3,\"groupType\":1,\"remainTime\":28,\"status\":2},{\"color\":5,\"groupNo\":8,\"groupType\":8,\"remainTime\":20,\"status\":2}]},{\"entranceDirection\":90,\"entranceGroupNum\":4,\"groupColors\":[{\"color\":5,\"groupNo\":5,\"groupType\":4,\"remainTime\":20,\"status\":2},{\"color\":5,\"groupNo\":6,\"groupType\":4,\"remainTime\":20,\"status\":2},{\"color\":6,\"groupNo\":7,\"groupType\":4,\"remainTime\":20,\"status\":3},{\"color\":3,\"groupNo\":12,\"groupType\":8,\"remainTime\":28,\"status\":2}]},{\"entranceDirection\":180,\"entranceGroupNum\":5,\"groupColors\":[{\"color\":3,\"groupNo\":9,\"groupType\":2,\"remainTime\":104,\"status\":2},{\"color\":3,\"groupNo\":10,\"groupType\":1,\"remainTime\":28,\"status\":2},{\"color\":3,\"groupNo\":11,\"groupType\":1,\"remainTime\":28,\"status\":2},{\"color\":5,\"groupNo\":15,\"groupType\":9,\"remainTime\":20,\"status\":2},{\"color\":5,\"groupNo\":16,\"groupType\":8,\"remainTime\":20,\"status\":2}]},{\"entranceDirection\":270,\"entranceGroupNum\":3,\"groupColors\":[{\"color\":3,\"groupNo\":4,\"groupType\":8,\"remainTime\":28,\"status\":2},{\"color\":5,\"groupNo\":13,\"groupType\":4,\"remainTime\":20,\"status\":2},{\"color\":5,\"groupNo\":14,\"groupType\":4,\"remainTime\":20,\"status\":2}]}],\"lat\":0.0,\"lon\":0.0,\"regionId\":370212,\"roadId\":17,\"timestamp\":1623897051696}}";
		
		
		
		JSONObject json=JSON.parseObject(colorJson);
		
		JSONObject recvObj=json.getJSONObject("recvObj");
		
 
		
		EntranceColor colorVo=JSON.parseObject(recvObj.toJSONString(), EntranceColor.class);
		System.out.println("====>colorVo="+JSON.toJSONString(colorVo));
		
		
		String planJson="{\"17:90:8:5\":68,\"17:0:1:3\":80,\"17:0:2:3\":130,\"17:90:8:3\":84,\"17:90:4:7\":4,\"17:0:1:5\":68,\"17:0:2:5\":18,\"17:90:4:5\":42,\"17:0:9:7\":4,\"17:0:1:7\":4,\"17:0:2:7\":4,\"17:0:8:3\":110,\"17:0:8:5\":42,\"17:0:9:3\":80,\"17:0:9:5\":68,\"17:180:2:3\":130,\"17:180:1:5\":68,\"17:180:1:3\":80,\"17:180:9:5\":68,\"17:270:8:5\":68,\"17:180:8:5\":27,\"17:180:9:3\":80,\"17:180:8:3\":125,\"17:180:2:7\":4,\"17:180:2:5\":18,\"17:180:1:7\":4,\"17:270:4:3\":121,\"17:270:4:5\":27,\"17:180:9:7\":4,\"17:270:4:7\":4,\"17:270:8:3\":84,\"17:90:4:3\":106}";
		
		
		ConcurrentHashMap<String, Integer>  map_currPlansWithNoGroupNo=JSONObject.parseObject(planJson, ConcurrentHashMap.class);
		
		
		List<Phase> ls=convertPhases("17",   colorVo, map_currPlansWithNoGroupNo);
		
		System.out.println("ls="+JSON.toJSONString(ls));
		
		
		
	}
	
	
	public static void test_color2()
	{
		
		String colorJson="{\"msgType\":\"color\",\"recvObj\":{\"alt\":0,\"entranceNum\":4,\"groups\":[{\"entranceDirection\":0,\"entranceGroupNum\":4,\"groupColors\":[{\"color\":7,\"groupNo\":1,\"groupType\":9,\"remainTime\":3,\"status\":2},{\"color\":3,\"groupNo\":2,\"groupType\":2,\"remainTime\":83,\"status\":2},{\"color\":3,\"groupNo\":3,\"groupType\":1,\"remainTime\":7,\"status\":2},{\"color\":3,\"groupNo\":8,\"groupType\":8,\"remainTime\":109,\"status\":2}]},{\"entranceDirection\":90,\"entranceGroupNum\":4,\"groupColors\":[{\"color\":7,\"groupNo\":5,\"groupType\":4,\"remainTime\":3,\"status\":2},{\"color\":7,\"groupNo\":6,\"groupType\":4,\"remainTime\":3,\"status\":2},{\"color\":3,\"groupNo\":7,\"groupType\":4,\"remainTime\":83,\"status\":2},{\"color\":3,\"groupNo\":12,\"groupType\":8,\"remainTime\":7,\"status\":2}]},{\"entranceDirection\":180,\"entranceGroupNum\":5,\"groupColors\":[{\"color\":3,\"groupNo\":9,\"groupType\":2,\"remainTime\":83,\"status\":2},{\"color\":3,\"groupNo\":10,\"groupType\":1,\"remainTime\":7,\"status\":2},{\"color\":3,\"groupNo\":11,\"groupType\":1,\"remainTime\":7,\"status\":2},{\"color\":7,\"groupNo\":15,\"groupType\":9,\"remainTime\":3,\"status\":2},{\"color\":3,\"groupNo\":16,\"groupType\":8,\"remainTime\":124,\"status\":2}]},{\"entranceDirection\":270,\"entranceGroupNum\":3,\"groupColors\":[{\"color\":3,\"groupNo\":4,\"groupType\":8,\"remainTime\":7,\"status\":2},{\"color\":7,\"groupNo\":13,\"groupType\":4,\"remainTime\":3,\"status\":2},{\"color\":7,\"groupNo\":14,\"groupType\":4,\"remainTime\":3,\"status\":2}]}],\"lat\":0.0,\"lon\":0.0,\"regionId\":370212,\"roadId\":17,\"timestamp\":1623907864886}}";
		
		
		
		JSONObject json=JSON.parseObject(colorJson);
		
		JSONObject recvObj=json.getJSONObject("recvObj");
		
 
		
		EntranceColor colorVo=JSON.parseObject(recvObj.toJSONString(), EntranceColor.class);
		System.out.println("====>colorVo="+JSON.toJSONString(colorVo));
		
		
		String planJson="{\"17:90:8:5\":68,\"17:0:1:3\":80,\"17:0:2:3\":130,\"17:90:8:3\":84,\"17:90:4:7\":4,\"17:0:1:5\":68,\"17:0:2:5\":18,\"17:90:4:5\":42,\"17:0:9:7\":4,\"17:0:1:7\":4,\"17:0:2:7\":4,\"17:0:8:3\":110,\"17:0:8:5\":42,\"17:0:9:3\":80,\"17:0:9:5\":68,\"17:180:2:3\":130,\"17:180:1:5\":68,\"17:180:1:3\":80,\"17:180:9:5\":68,\"17:270:8:5\":68,\"17:180:8:5\":27,\"17:180:9:3\":80,\"17:180:8:3\":125,\"17:180:2:7\":4,\"17:180:2:5\":18,\"17:180:1:7\":4,\"17:270:4:3\":121,\"17:270:4:5\":27,\"17:180:9:7\":4,\"17:270:4:7\":4,\"17:270:8:3\":84,\"17:90:4:3\":106}";
		
		
		ConcurrentHashMap<String, Integer>  map_currPlansWithNoGroupNo=JSONObject.parseObject(planJson, ConcurrentHashMap.class);
		
		
		List<Phase> ls=convertPhases("17",   colorVo, map_currPlansWithNoGroupNo);
		
		System.out.println("ls="+JSON.toJSONString(ls));
		
		
	}
	
	public static void main(String [] args)
	{
		test_color2();
		
	}
	

}
