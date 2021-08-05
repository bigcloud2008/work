package com.tusvn.ccinfra.communication.netty.handler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

import com.alibaba.druid.util.HexBin;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tusvn.ccinfra.api.data.storage.RedisUtils;
import com.tusvn.ccinfra.communication.netty.domain.EntranceColor;
import com.tusvn.ccinfra.communication.netty.domain.EntranceLightGroup;
import com.tusvn.ccinfra.communication.netty.domain.LaneControlStatus;
import com.tusvn.ccinfra.communication.netty.domain.LaneControler;
import com.tusvn.ccinfra.communication.netty.domain.LaneFunc;
import com.tusvn.ccinfra.communication.netty.domain.LaneFuncStatus;
import com.tusvn.ccinfra.communication.netty.domain.LightGroupColor;
import com.tusvn.ccinfra.communication.netty.domain.LightVo;
//import com.tusvn.ccinfra.api.utils.DataConversionUtil;
//import com.tusvn.ccinfra.api.utils.RedisClusterUtils;
import com.tusvn.ccinfra.communication.netty.domain.Plan;
import com.tusvn.ccinfra.communication.netty.domain.PlanColorInfo;
import com.tusvn.ccinfra.communication.netty.domain.PlanColorStep;
import com.tusvn.ccinfra.communication.netty.domain.PlanLightGroup;
import com.tusvn.ccinfra.communication.netty.domain.SignalControlMode;
import com.tusvn.ccinfra.communication.netty.domain.SignalStatus;
import com.tusvn.ccinfra.communication.netty.model.SpatData;
import com.tusvn.ccinfra.communication.netty.service.LoginInfo;
import com.tusvn.ccinfra.communication.netty.service.SpatService;
import com.tusvn.ccinfra.communication.netty.util.LogWriter;
import com.tusvn.ccinfra.communication.netty.util.ParseUtils;
import com.tusvn.ccinfra.communication.netty.util.SpatUtils;
import com.tusvn.ccinfra.config.ConfigProxy;
import com.tusvn.ccinfra.config.uitls.CfgUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * 信号灯数据处理---主逻辑入口
 * 
 * @author liaoxb
 *
 */
public class SpatHandler extends BasicChannelHandler<DatagramPacket> {
	
	////protected org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpatHandler.class);
	private static final Logger logger = LogManager.getLogger(SpatHandler.class);
	
	
	private static final ExecutorService EXECUTOR_QUERY_SERVICE  = Executors.newFixedThreadPool(ConfigProxy.getIntOrDefault("spat.query.thread.num",   1));
	
	private static final ExecutorService EXECUTOR_PARSE_SERVICE  = Executors.newFixedThreadPool(ConfigProxy.getIntOrDefault("spat.parse.thread.num",   100));
	
	
	
	////public static final Integer REGIONID=ConfigProxy.getIntOrDefault("spat.region", 110115);/////大兴区
	
	public static final String  ROADS   =CfgUtils.ROADS;/////要处理的28个路口id
	
	
	
	
	

	public SpatHandler(int port) {
		super(port);
		this.port = port;
	}
	
	
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
    	
    	logger.info("=================>in channelActive");
 
    	////////////1.初始化发送当前周期配时方案查询
    	
//    	EXECUTOR_QUERY_SERVICE.execute(()->{
//    		sendQuery(ctx);
//		});
    	
    }
    

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
		
//		InetSocketAddress ipSocket = (InetSocketAddress)ctx.channel().remoteAddress();
//	    String clientIp = ipSocket.getAddress().getHostAddress();
	    
		logger.info("=================>in channelRead0");///, ip="+clientIp
 
		
		System.out.println("=================>in channelRead0");
		
		long currentTimeMillis = System.currentTimeMillis();
		ByteBuf in = msg.content();
		if (in.readableBytes() < 2) {
			
			logger.info("=================>in channelRead0, 可读字节<2字节");
			return;
		}
		// 判断协议头是c0
		
		byte first=in.getByte(0);
		
		if (192 == (first & 0xFF)) {
			
			logger.info("=====>a111111111111111111111111111");
			
			// 我们标记一下当前的readIndex的位置
			in.markReaderIndex();
			in.readByte();
			int len = 1;
			// 循环读取，读到c0结束
			while (in.readableBytes() > 0) {
				len++;
				if (192 == (in.readByte() & 0xFF)) {
					
					in.resetReaderIndex();
					
					ByteBuf directBuffer =null;
					
					try {
						
						long x0=System.currentTimeMillis();
						
						directBuffer=ctx.channel().alloc().directBuffer(len);
						
						in.readBytes(directBuffer);
						
						dataProcess(ctx, directBuffer, currentTimeMillis);
						
						
						long x1=System.currentTimeMillis();
						
						logger.info("===============>in dataProcess, usetime x1-x0="+(x1-x0)+" ms"  );
					
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						logger.error("in channelRead0", e);
					}
					finally {
						ReferenceCountUtil.release(directBuffer);
					}
					
					return;
				}
			}
			in.resetReaderIndex();
			return;
		}
		else
			logger.info("=====>首字节非192,first="+first);
	}

	private void dataProcess(ChannelHandlerContext ctx, ByteBuf in, long currentTimeMillis) throws Exception {
		
		ByteBuf copy_in  =in.copy();
		byte[] copy_dst =new byte[copy_in.readableBytes()];
		try {
			copy_in.getBytes(0, copy_dst);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("in copy in", e);
		}
		finally {
			ReferenceCountUtil.release(copy_in);
		}
		//////////////////////////////
		
		
		// 获取协议头一位
		int head = in.getByte(0) & 0xFF;
		
		// 获取协议头最后一位
		int tail = in.getByte(in.readableBytes() - 1) & 0xFF;
		
		// 判断头和尾是不是0xc0
		if (!(192 == head && 192 == tail)) {
			
			logger.info("===============>head="+head+", tail="+tail);
			
			return;
		}
		////LogWriter.printLog(logger, "SpatHandler -> hex16: {}", in, ConfigProxy.getBooleanOrDefault("printSpatLog", false));
		

		// 替换0xC0和0xDB
		byte[] byteData = new byte[in.readableBytes()];
		in.readBytes(byteData);
		String hexData = HexBin.encode(byteData);
		// 替换次数
		
		int countMatches = StringUtils.countMatches(hexData, "DBDC");
		countMatches += StringUtils.countMatches(hexData, "DBDD");
		
		// 替换特殊字符
		hexData = RegExUtils.replaceAll(hexData, "DBDC", "C0");
		hexData = RegExUtils.replaceAll(hexData, "DBDD", "DB");
		
		in.writeBytes(HexBin.decode(hexData));
		
		// 开始位
		int begin = in.readByte() & 0xff; ////C0
		
		////////logger.info("begin="+begin);

		/**
		 * 帧头 开始
		 */
		/********
		链路码	        2
		发送方标识	    7
		接收方标识	    7
		时间戳	        6
		生存时间	    1
		协议版本	    1
		操作类型	    1 
		对象标识	    2
		签名标记	    1
		保留	        3
		消息内容	    
		签名证书
		*******/

		// 链路码  2 bytes
		byte[] linkCode=new byte[2];
		in.readBytes(linkCode);
		
		///////ParseUtils.printByteArray("linkCode", linkCode);
		
		
		// 发送id  ----	发送方标识：发送方唯一身份，长度7字节。编制规则为：行政区划代码+类型+编号
		byte[] sendId = new byte[7];
		in.readBytes(sendId);
		
		ParseUtils.printByteArray("sendId", sendId);
		
		
		////得到发送者的设备或平台唯一编号
		int origin_senderSn=SpatUtils.getDeviceSn(sendId);   /////////------------------这里广播方式是0xffff
		
		int senderSn=origin_senderSn;
		
		////logger.info("===>origin_senderSn="+origin_senderSn);
		
//		if(senderSn==0)
//			senderSn=9;
		////////////////////////////////////////
		
		
		// 接收id
		byte[] receiveId = new byte[7];
		in.readBytes(receiveId);
		
		ParseUtils.printByteArray("receiveId", receiveId);
		
		
		int receiverSn=SpatUtils.getDeviceSn(receiveId);  /////////////////---------代表路口id
		
		
		////logger.info("===>senderSn="+senderSn+",receiverSn="+receiverSn);
		
		int roadId=0;
		
		
		if(senderSn==0xffff)
			roadId=receiverSn;
		else
			roadId=senderSn;
		
		final String s_roadId=""+roadId;
		
		////////////////////////////////////////

		// UTC 时间 6
		byte[] arr_TUCTime =  new byte[6];
		in.readBytes(arr_TUCTime);
		
		ParseUtils.printByteArray("arr_TUCTime", arr_TUCTime);
		
		Long utcTime=SpatUtils.sixBytes2Long(arr_TUCTime);
		////logger.info("utcTime="+utcTime);
		
		// TTL  1
		byte lifecycle = in.readByte();
		
		// 版本号（0x10）  1
		byte versionNumber = in.readByte();
		
		// 操作类型  1
		int operationType = in.readByte() & 0xFF;
		logger.info("operationType="+operationType);
		
		
		///////0x83===131 ======0x83	查询应答	对查询请求的应答
		///////0x87=====135=====广播
		if(operationType!=0x83 && operationType!=0x87)
		{
			logger.info("==========>unknown operationType:"+operationType);
			return ;
		}
		
		
		// 对象标识   2
		byte[] arr_operation =  new byte[2];
		in.readBytes(arr_operation);
		
		
		//////0x0103---------259------描述当前信号灯组的灯色和剩余时间
		//////0x0301---------769------描述信号机当前运行方案的灯色及时长
		short operationObject=convertOperatObj(arr_operation);
		logger.info("=====>operationType=["+operationType+"]operationObject=["+operationObject+"] origin_senderSn="+origin_senderSn+", senderSn="+senderSn+", receiverSn="+receiverSn+", utcTime="+utcTime+",currTime="+System.currentTimeMillis());
 
		LogWriter.printLog(logger, "[NEED]operationType="+operationType+",operationObject="+operationObject+",senderSn="+senderSn+",receiverSn="+receiverSn+",utcTime="+utcTime+",currTime="+System.currentTimeMillis()+",SpatHandler -> hex16: {}", in, ConfigProxy.getBooleanOrDefault("printSpatLog", true));
		
		
		// 签名标记  1
		byte signFlag =in.readByte();
		
		//保留 3个字节
		byte[] reserved =  new byte[3];
		in.readBytes(reserved);
		
		
		/**
		 * 帧头 结束
		 */
		// 消息长度
		int messageLength = in.readShortLE();
		logger.info("messageLength="+messageLength);
		
		////////////////////////////////////
		Integer regionId=SpatUtils.threeBytes2Int(sendId);
		logger.info("send.regionId="+regionId);
		
		Integer recv_regionId=SpatUtils.threeBytes2Int(receiveId);
		logger.info("recv.regionId="+recv_regionId);
		

		// 解析
		long b0=System.currentTimeMillis();
		LightVo lightVo=executeParse(ctx, in, utcTime, operationType, operationObject,  regionId, roadId, currentTimeMillis);
		long b1=System.currentTimeMillis();
		logger.info("===============>b1111111111111111111111, type="+lightVo.getMsgType()+"= usetime b1-t0="+(b1-b0)+" ms"  );
		
		
		
		////// 开始处理服务端的消息响应, 解析海信平台返回的响应消息
		EXECUTOR_PARSE_SERVICE.execute(()->{
			
			
			if((CfgUtils.map_crossId.get(s_roadId))!=null) //////////------接收到的消息路口在1----28之内，是有效的
			{
				long e0=System.currentTimeMillis();
				
				sendToOtherParty(s_roadId, operationObject, copy_dst );/////发送给百度
				
				long e1=System.currentTimeMillis();
				
				logger.info("===============>b1111111111111111111111, usetime e1-e0="+(e1-e0)+" ms"  );
				
				
				doParse(ctx, lightVo);   //////----主逻辑
				
				
				long e2=System.currentTimeMillis();
				
				logger.info("===============>b1111111111111111111111, type="+lightVo.getMsgType()+" usetime e2-e0="+(e2-e0)+" ms"  );
				
			}
			else   ///////////-----无效路口跳过
				logger.info("==========>[SKIP]unknown origin_senderSn="+origin_senderSn+", skip this message");
				

			
    		
    		
		});
		
		
	}
	
	
	////------------2021-4-13 liaoxb add发送给第三方：百度
	public static void sendToOtherParty(String crossId, short operationObject, byte [] copy_dst)
	{
		try {
			
//			String crossId = roadIdMap.get(sender_id);  ///////根据发送者设备id得到路口id
//			if(crossId==null)
//				crossId=sender_id;
//			
			
			
			JSONObject obj=new JSONObject();
			
			String type="";
			
			if(operationObject==0x0101)  ///信号机运行状态
				type="013";
			else if(operationObject==0x0102)  ///信号控制方式
				type="023";
			else if(operationObject==0x0103)  ///信号灯灯色状态
				type="030";
			else if(operationObject==0x0201) ////车道功能状态
				type="043";
			else if(operationObject==0x0202)  ////车道/匝道控制信息
				type="053";
			else if(operationObject==0x0301)  ////当前信号方案色步信息
				type="063";
			else if(operationObject==0x0302)  ////下一个周期信号方案色步信息
				type="073";
			
 
			obj.put("type", type);
			
			obj.put("cross_id", crossId);/////字串
			
			obj.put("timestamp", System.currentTimeMillis());
			
			
			String content=SpatUtils.encode(copy_dst);
			
			obj.put("content", content);
			
			SpatService.writeKafkaForOtherParty(crossId, obj);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("in sendToOtherParty:"+e.getMessage() , e );
		}
		
	}
	
	
	private void doParse(ChannelHandlerContext ctx, LightVo lightVo)
	{
		String msgType=null;
		
		
		if(lightVo==null)
		{
			logger.info("lightVo is null!");
			return ;
		}
		
		try
		{
			/////////1.spat实时灯态写redis+kafka
			
			msgType=lightVo.getMsgType();
			
			
			
			if(msgType!=null && msgType.equals("color"))
			{
				
				EntranceColor colorVo=(EntranceColor)lightVo.getRecvObj();
				
				String s_roadId=""+colorVo.getRoadId();
				
				ConcurrentHashMap<String, Integer>  single_map_plans_nogroupno=LoginInfo.map_currPlansWithNoGroupNo.get(colorVo.getRoadId());
				logger.info("====road="+s_roadId+"===>recv color, single_map_plans_nogroupno="+JSON.toJSONString(single_map_plans_nogroupno));
				
				
//				if(single_map_plans_nogroupno==null) ////从redis中获取最新的plan
//				{
//					String redis_key="SPAT:CURRPLAN:"+colorVo.getRoadId();
//					try {
//						String tmp_plan_str=RedisUtils.Strings.get(redis_key);
//						
//						if(tmp_plan_str!=null)
//							single_map_plans_nogroupno=JSONObject.parseObject(tmp_plan_str,  ConcurrentHashMap.class);
//						
//						
//						logger.info("=======>read from redis, single_map_plans_nogroupno="+JSON.toJSONString(single_map_plans_nogroupno));
//						
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//						logger.error("read redis, redis_key="+redis_key, e);
//					}
//					
//				}
				
				
				if(single_map_plans_nogroupno!=null && single_map_plans_nogroupno.size()>0)
				{
					long t1=System.currentTimeMillis();
					SpatData spData=SpatService.convertSpatData(colorVo, single_map_plans_nogroupno);
					long t2=System.currentTimeMillis();
					
					
					logger.info("===============>b1111111111111111111111, usetime t2-t1="+(t2-t1)+" ms"  );
					
					
					
				
					if(spData==null)
					{
						logger.error("convertSpatData SpatData error.");
						return ;
					
					}
				
				
					/////////////1.写redis
					SpatService.writeRedis(spData); //////里面时间单位为秒
					
					long t3=System.currentTimeMillis();
 
					logger.info("===============>a1111111, usetime t3-t2="+(t3-t2)+" ms"  );
					
				
					/////////////2.写kafka
					Integer roadId  =colorVo.getRoadId();
					Integer regionId=colorVo.getRegionId();
					
					
					SpatData converted_spData=SpatService.transferTime(spData);/////////时间单位转换为0.1s
	 
					///////////--------------------------------------
					Long finTime=System.currentTimeMillis();
					
					logger.info("=s_roadId="+s_roadId+"=[NEED]msgTime="+lightVo.getMsgTime()+",recvTime="+lightVo.getRecvTime()+",finTime="+finTime+",usetime="+(finTime-lightVo.getRecvTime())+"ms=====>spData="+JSON.toJSONString(spData));
					
					
				
					SpatService.writeKafka(roadId, regionId, converted_spData);
					
					
					long t4=System.currentTimeMillis();
					 
					logger.info("===============>a1111111,usetime t4-t3="+(t4-t3)+" ms"  );
				}
				else
				{
					logger.error("LoginInfo.map_currPlansWithNoGroupNo is null, skip convertSpatData,colorVo="+JSON.toJSONString(colorVo));
				}
				
			}
			else if(msgType!=null && msgType.equals("currPLan"))
			{
				Plan plan=(Plan)lightVo.getRecvObj();
				
				Integer roadId=plan.getRoadId();
				
				ConcurrentHashMap<String, Integer> map_plans=SpatUtils.parsePlanTimes(  ""+roadId,   plan);
				logger.info("===>road="+roadId+",map_plans="+JSON.toJSONString(map_plans));
				
				

				ConcurrentHashMap<String, Integer>  single_map_plans_nogroupno=SpatUtils.parsePlanTimesWithNoGroupNo(  ""+roadId, map_plans);
				logger.info("===>road="+roadId+",single_map_plans_nogroupno="+JSON.toJSONString(single_map_plans_nogroupno));
				
				
 
				SpatUtils.sort_map(map_plans);
				
 
				SpatUtils.sort_map(single_map_plans_nogroupno);
				
				
				LoginInfo.map_currPlans.putAll(map_plans);
				
				///////LoginInfo.map_currPlansWithNoGroupNo.putAll(single_map_plans_nogroupno);
				
				LoginInfo.map_currPlansWithNoGroupNo.put(roadId, single_map_plans_nogroupno);
				
				//////////////////////////
				try {
					String redis_key="SPAT:CURRPLAN:"+plan.getRoadId();
					RedisUtils.Strings.set(redis_key, JSON.toJSONString(single_map_plans_nogroupno));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error("in write curr plan to redis ", e);
				}
				
			}
			else if(msgType!=null && msgType.equals("nextPLan"))
			{
				Plan plan=(Plan)lightVo.getRecvObj();
				
				Integer roadId=plan.getRoadId();
				
				ConcurrentHashMap<String, Integer> map_plans=SpatUtils.parsePlanTimes(  ""+roadId,   plan);
				logger.info("===>map_plans="+JSON.toJSONString(map_plans));
				
				
 
				ConcurrentHashMap<String, Integer>  single_map_plans_nogroupno=SpatUtils.parsePlanTimesWithNoGroupNo(  ""+roadId, map_plans);
				logger.info("===>single_map_plans_nogroupno="+JSON.toJSONString(single_map_plans_nogroupno));
				
				

				SpatUtils.sort_map(map_plans);
				

				SpatUtils.sort_map(single_map_plans_nogroupno);
				
				
				LoginInfo.map_nextPlans.putAll(map_plans);
				/////////LoginInfo.map_nextPlansWithNoGroupNo.putAll(single_map_plans_nogroupno);
				
				LoginInfo.map_nextPlansWithNoGroupNo.put(roadId, single_map_plans_nogroupno);
				
				
				////////////////////////////
				try {
					String redis_key="SPAT:NEXTPLAN:"+plan.getRoadId();
					RedisUtils.Strings.set(redis_key, JSON.toJSONString(single_map_plans_nogroupno));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error("in write next plan to redis ", e);
				}
			}
			else
				logger.info("=============>in doParse, skip msgType="+msgType+", lightVo="+JSON.toJSONString(lightVo));
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			
			logger.error("in doParse", ex);
		}
		
		
		
	}

	/**********
	 * 
	 * int type: 操作类型  长度1字节  
	 * 
	 * byte [2]  operationObject:  操作对象， 2个字节  由对象分类编码（1字节）+对象名称编码（1字节）构成
	 * 
	 * **/
	private LightVo executeParse(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer regionId, Integer roadId, long currentTimeMillis) {
		LightVo res=null;
		
		try {
//			MessageType messageType = MessageType.getByIntVal(type);
//			// 消息类型有效性判断
//			if (messageType != MessageType.SPAT) {
//				return;
//			}
			
			
			if(operationObject==0x0101)  ////信号机运行状态/01	0x0101	描述信号机当前运行状态。如：正常工作状态，未工作状态，故障状态等
			{
				res=parseLightRunStatus(  ctx,   in,   UTCTime,   type,   operationObject, regionId, roadId);
			
			}
			else if(operationObject==0x0102)  ////信号控制方式/02	0x0102	描述信号机当前控制方式，如：黄闪控制、多时段控制、手动控制、感应控制、无电缆协调控制、单点优化控制、公交信号优先、紧急事件优先等
			{
				res=parseLightController(  ctx,   in,   UTCTime,   type,   operationObject, regionId, roadId);
			
			}
			else if(operationObject==0x0103)  ////信号灯灯色状态/03	0x0103	描述当前信号灯组的灯色和剩余时间
			{
				res=parseLightColor(  ctx,   in,   UTCTime,   type,   operationObject,   regionId, roadId,  currentTimeMillis);
			
			}
			else if(operationObject==0x0301)  ////当前信号方案色步信息/01	0x0301	描述信号机当前运行方案的灯色及时长-----------------important
			{
				res=parseCurrlightPlan(  ctx,   in,   UTCTime,   type,   operationObject, regionId, roadId);
			
			}
			else if(operationObject==0x0302)  ////下一个周期信号方案色步信息/02	0x0302	描述信号机下一个周期将要运行新方案的灯色及时长
			{
				res=parseNextlightPlan(  ctx,   in,   UTCTime,   type,   operationObject, regionId, roadId);
			}
			else if(operationObject==0x0201)  ////车道功能状态
			{
				res=parseLaneFuncStatus(  ctx,   in,   UTCTime,   type,   operationObject, regionId, roadId);
			}
			else if(operationObject==0x0202)  ////车道/匝道控制信息
			{
				res=parseLaneControlStatus(  ctx,   in,   UTCTime,   type,   operationObject, regionId, roadId);
			}
			else
				logger.warn("unknown operateObj:"+operationObject);
			
			
		} catch (Exception e) {
			logger.error("解析红绿灯数据出错," + e.getMessage(), e);
			e.printStackTrace();
		} finally {
		}
		
		
		return res;
		
	}
	
	
	////////////------------------
//    /****
//     * 读出4字节的时间戳
//     * 
//     * */	
//	public static long convertUtcTime(byte [] arr_time)
//	{
//		ByteBuf buf = Unpooled.buffer(arr_time.length);
//		
//		buf.writeBytes(arr_time);
//		
//		return buf.readLongLE();
//		
//	}
	
	public static short convertOperatObj(byte [] arr_operation)
	{
		short operObj=0;
		ByteBuf buf =null;
		
		try {
			buf=Unpooled.buffer(arr_operation.length);
			
			buf.writeBytes(arr_operation);
			
			operObj=buf.readShortLE();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("in convertOperatObj", e);
		}
		finally {
			ReferenceCountUtil.release(buf);
		}
		
		return operObj;
		
	}
	
	/*******
	 * A.1.2　信号机运行状态查询应答
	 * 
	 * 
	 * **/
	public LightVo parseLightRunStatus(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer regionId, Integer roadId)
	{
		LightVo vo=new LightVo();
		
		// 经度
		float longitude = in.readFloatLE();///4
	 
		// 纬度
		float latitude = in.readFloatLE();////4
 
		
		// 海拔
		int altitude = in.readShortLE();////2 bytes
 
		
		long time=in.readLongLE();////////4 bytes
		
		/*****
		 *  0：无效
			1：工作正常
			2：故障状态
			3：其他
		 * 
		 * 
		 * **/
		int  status=in.readByte(); /////1 byte 
		
		SignalStatus  signalStatus=new SignalStatus(regionId, roadId,  longitude,   latitude,   altitude,    time,   status); 
		
		vo.setMsgType("runstatus");
		vo.setRecvObj(signalStatus);
		
		logger.info("=======>in parseLightRunStatus: "+JSON.toJSONString(vo));
		
		return vo;
	}
	
	/****
	 * A.2.2　信号控制方式查询应答
	 * 
	 * 
	 * **/
	public LightVo parseLightController(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer regionId, Integer roadId)
	{
		LightVo vo=new LightVo();
 
		
		// 经度
		float longitude = in.readFloatLE();///4
 
		// 纬度
		float latitude = in.readFloatLE();////4
 
		
		// 海拔
		int altitude = in.readShortLE();////2 bytes
 
		
		long time=in.readLongLE();////////4 bytes
		
		/*****
		 * 
整数，取值范围：
1：黄闪控制
2：多时段控制
3：手动控制
4：感应控制
5：无电缆协调控制
6：单点优化控制
7：公交信号优先
8：紧急事件优先
9：其他

		 * 
		 * **/
		int  mode=in.readByte(); /////1 byte 
 
		
		SignalControlMode  signalControlMode=new SignalControlMode( regionId, roadId, longitude,   latitude,   altitude,    time,   mode); 
		
		vo.setMsgType("controller");
		vo.setRecvObj(signalControlMode);
		
		logger.info("=======>in parseLightController: "+JSON.toJSONString(vo));
		
		
		
		return vo;
		
	}
	
	/*******
	 * A.3　信号灯灯色状态
	 * 
	 * 
	 * 
	 * **/
	public LightVo parseLightColor(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer regionId, Integer roadId, long currentTimeMillis)
	{
		LightVo vo=new LightVo();
		
		
		// 路口Id
//		String s_roadId = ""+roadId;/////roadIdMap.get(port);  ///////////////////----------------根据端口得到路口id这里可能需要修改
		
//		Integer roadId=null;
//		
//		if(s_roadId!=null)
//			roadId=Integer.parseInt(s_roadId);
		
		logger.info("=====>roadId="+roadId);
		
		
		/**
		 * 帧内容 开始
		 */
		// 经度
		float longitude = in.readFloatLE();///4
 
		// 纬度
		float latitude = in.readFloatLE();////4
 
		
		// 海拔
		int altitude = in.readShortLE();////2 bytes
		
		logger.info("====>longitude="+longitude);
		logger.info("====>latitude="+latitude);
		logger.info("====>altitude="+altitude);
 
		
		// 路口进口数量
		int importDirectionNum = in.readByte() & 0xffff; /////1 byte
		
		logger.info("=====>importDirectionNum="+importDirectionNum);
		
		EntranceColor entranceColor=new EntranceColor();
		
		entranceColor.setRoadId(roadId);
		entranceColor.setRegionId(regionId);
		entranceColor.setTimestamp(currentTimeMillis);
		
		entranceColor.setLon(longitude);
		entranceColor.setLat(latitude);
		entranceColor.setAlt(altitude);
		entranceColor.setEntranceNum(importDirectionNum);

		List<EntranceLightGroup> groups=new ArrayList<>();
		
 
		for (int i = 0; i < importDirectionNum; i++) {
 
			// 进口方向 i
			int directionI = in.readShortLE();  //////进口方向 2字节,整数，以地理正北方向为起点的顺时针旋转角度，单位为度（°）。其中取值0表示0度到1度，包含0度且不包含1度，下同
 
			logger.info("=roadId=["+roadId+"]==i="+i+"==>directionI="+directionI);
			
			// 进口灯组数量
			int phaseNum = in.readByte() &0xffff ;   //////1字节
			
			EntranceLightGroup group=new EntranceLightGroup();
			group.setEntranceDirection(directionI);
			group.setEntranceGroupNum(phaseNum);
			
			List<LightGroupColor> groupColors=new ArrayList<>();
 
			
			for (int j = 0; j < phaseNum; j++) {
 
				
				// 灯组编号id j
				int groupNo = in.readByte() &0xff;    /////1字节
 
				
				// 灯组类型 j
				int groupType = in.readByte()  &0xff;   ////1字节
 
				
				// 灯色 j
				byte color   = in.readByte();    ////1字节
				
				
				String s_color=SpatUtils.byteToBit(color);///// 0000,0000
				
				
				
				////////////////////////////////////
				
				String  bit1bit0=s_color.substring(6,8);  ////Bit1~Bit0用于表示红色发光单元
				String  bit3bit2=s_color.substring(4,6);  ////Bit3~Bit2用于表示黄色发光单元
				String  bit5bit4=s_color.substring(2,4);  ////Bit5~Bit4用于表示绿色发光单元
				String  bit7bit6=s_color.substring(0,2);  ////resv
				
				
				logger.info("====roadId="+roadId+"======>directionI="+directionI+",groupType="+groupType+",bit1bit0="+bit1bit0+", bit3bit2="+bit3bit2+", bit7bit6="+bit7bit6);
//				logger.info("====>bit3bit2="+bit3bit2);
//				logger.info("====>bit5bit4="+bit5bit4);
//				logger.info("====>bit7bit6="+bit7bit6);
				
				////////////////////////////////////
				
				// 剩余时间 1 byte
				int remainTime = in.readByte() & 0xff;
				
				logger.info("=roadId="+roadId+"=j="+j+"===>groupNo="+groupNo+",directionI="+directionI+",groupType="+groupType+", color="+color+", s_color="+s_color+",remainTime="+remainTime);
				
				////logger.info("=====>remainTime="+remainTime);
				
				if(groupType>=1 && groupType<=12) ////当灯组类型为1~12时，Bit1~Bit0用于表示红色发光单元，Bit3~Bit2用于表示黄色发光单元，Bit5~Bit4用于表示绿色发光单元，Bit7~ Bit6保留
				{
					LightGroupColor groupColor=new LightGroupColor();
					
					groupColor.setGroupNo(groupNo);
					groupColor.setGroupType(groupType);
					
					if(bit1bit0.equals("10"))
					{
						groupColor.setColor(3);////红亮
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(2);
						
						logger.info("==roadId="+roadId+"==>red color1="+bit1bit0+", remainTime="+remainTime);
					}
					else if(bit1bit0.equals("11"))
					{
						groupColor.setColor(2);////红闪
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(3);
						
						logger.info("==roadId="+roadId+"==>red color2="+bit1bit0+", remainTime="+remainTime);
					}
					else
					{
						//////////logger.info("====>unknown red color="+bit1bit0+", remainTime="+remainTime);
					}
					///////////////////////////////////
					
					if(bit3bit2.equals("10"))
					{
						groupColor.setColor(7);////黄亮
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(2);
						
						logger.info("==roadId="+roadId+"==>yellow color1="+bit3bit2+", remainTime="+remainTime);
					}
					else if(bit3bit2.equals("11"))
					{
						groupColor.setColor(8);////黄闪
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(3);
						
						logger.info("==roadId="+roadId+"==>yellow color2="+bit3bit2+", remainTime="+remainTime);
					}
					else
					{
						///////////logger.info("====>unknown yellow color="+bit3bit2+", remainTime="+remainTime);
					}
 
					if(bit5bit4.equals("10"))
					{
						groupColor.setColor(5);////绿亮
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(2);
						
						logger.info("==roadId="+roadId+"==>green color1="+bit5bit4+", remainTime="+remainTime);
					}
					else if(bit5bit4.equals("11"))
					{
						groupColor.setColor(6);////绿闪
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(3);
						
						logger.info("==roadId="+roadId+"==>green color2="+bit5bit4+", remainTime="+remainTime);
					}
					else
					{
						/////////////logger.info("====>unknown green color="+bit1bit0+", remainTime="+remainTime);
					}
					
					
					groupColors.add(groupColor);

				}
				else /////当灯组类型为13~15时，Bit1~ Bit0用于表示禁止通行信号发光单元，Bit3~ Bit2用于表示过渡信号发光单元，Bit5~Bit4用于表示通行信号发光单元，Bit7~Bit6保留
				{
					logger.info("========roadId="+roadId+"==========>skip groupType="+groupType);
					
				}
			}
			
			group.setGroupColors(groupColors);
			
			groups.add(group);
 
		}
		
		entranceColor.setGroups(groups);
		
		/////////////////////////////////
		vo.setMsgType("color");
		vo.setRecvObj(entranceColor);
		
		vo.setMsgTime(UTCTime);
		vo.setRecvTime(currentTimeMillis);
		
		logger.info("======>[COLOR]road=["+roadId+"], vo="+JSON.toJSONString(vo));
		
		return vo;
	}
	
	
	
	
	
	/********
	 * 
	 * A.4　车道功能状态
	 * 
	 * **/
	public LightVo parseLaneFuncStatus(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer regionId, Integer roadId)
	{
		LightVo vo=new LightVo();
		
		
		// 路口Id
////		String roadId = ""+receiverSn;////roadIdMap.get(port);
 
		// 经度
		float longitude = in.readFloatLE();///4
 
		// 纬度
		float latitude = in.readFloatLE();////4
 
		
		// 海拔
		int altitude = in.readShortLE();////2 bytes
 
		

		int changedFuncLaneNum       =in.readByte();//////功能可变车道进口数量  1 byte
		
		//////logger.info("======>entrance="+entrance);
		
		LaneFuncStatus laneFuncStatus=new LaneFuncStatus();
		
		laneFuncStatus.setRegionId(regionId);
		laneFuncStatus.setRoadId(roadId);
		laneFuncStatus.setTimestamp(UTCTime);
		
		laneFuncStatus.setLon(longitude);
		laneFuncStatus.setLat(latitude);
		laneFuncStatus.setAlt(altitude);
		
		
		List<LaneFunc> laneFuncs=new ArrayList<LaneFunc>();
 
		
		for(int i=0;i<changedFuncLaneNum;i++)
		{
			int entranceDirection=in.readShortLE();////进口方向 2bytes
			int entranceGroupNum =in.readByte();/////进口灯组数量	1 byte
			
			LaneFunc laneFunc=new LaneFunc();
			laneFunc.setInDirection(entranceDirection);
			laneFunc.setInLaneNum(entranceGroupNum);
			
			List<Integer> laneInfos=new ArrayList<>();
			
			for(int j=0;j<entranceGroupNum;j++)
			{
				int laneType=in.readShortLE();
				
				laneInfos.add(laneType);
			}
			
			laneFunc.setLaneInfos(laneInfos);
			
			laneFuncs.add(laneFunc);
		}
		
		laneFuncStatus.setLaneFuncs(laneFuncs);
		
 
		vo.setMsgType("laneFuncStatus");
		vo.setRecvObj(laneFuncStatus);
		
		logger.info("======>[parseLaneFuncStatus]road=["+roadId+"], data="+JSON.toJSONString(vo));
		
		return vo;
	}
	
	
	
	/********
	 * A.5　车道/匝道控制状态信息
	 * 
	 * 
	 * 
	 * **/
	public LightVo parseLaneControlStatus(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer regionId, Integer roadId)
	{
		LightVo vo=new LightVo();
 
		
		// 路口Id
////		String roadId = ""+receiverSn;////roadIdMap.get(port);
 
		// 经度
		float longitude = in.readFloatLE();///4
 
		// 纬度
		float latitude = in.readFloatLE();////4
 
		
		// 海拔
		int altitude = in.readShortLE();////2 bytes
 
		

		int controlerLaneNum       =in.readByte();//////功能可变车道进口数量  1 byte
		
		//////logger.info("======>entrance="+entrance);
		
		LaneControlStatus laneControlerStatus=new LaneControlStatus();
		
		laneControlerStatus.setRegionId(regionId);
		laneControlerStatus.setRoadId(roadId);
		laneControlerStatus.setTimestamp(UTCTime);
		
		laneControlerStatus.setLon(longitude);
		laneControlerStatus.setLat(latitude);
		laneControlerStatus.setAlt(altitude);
		
		
		List<LaneControler> laneControllers=new ArrayList<LaneControler>();
 
		
		for(int i=0;i<controlerLaneNum;i++)
		{
			LaneControler controler=new LaneControler();
			
			
			int  tmp_type=in.readByte();
 
			int  laneNo  =in.readByte();
			
			int laneDirection=in.readShortLE();////车道/匝道方向
			int laneSignal =in.readByte();/////车道/匝道信号   1:开放， 2：关闭
			
			
			controler.setType(tmp_type);
			controler.setLaneNo(laneNo);
			controler.setLaneDirection(laneDirection);
			controler.setLaneSignal(laneSignal);
			
			laneControllers.add(controler);
			 
		}
		
		laneControlerStatus.setLaneControlers(laneControllers);
		
 
		vo.setMsgType("laneControlerStatus");
		vo.setRecvObj(laneControlerStatus);
		
		logger.info("======>[parseLaneFuncStatus]road=["+roadId+"], vo="+JSON.toJSONString(vo));
		
		return vo;
	}
	
	
	
	
	/*******
	 * A.6.2　当前信号方案色步信息查询应答
	 * 
	 * 
	 * 灯组类型
	 * 
	 *   13：有轨电车专用信号灯（直行）
14：有轨电车专用信号灯（左转）
15：有轨电车专用信号灯（右转）

	 * 
	 * 
	 * 1．按比特位定义灯组发光单元
1）当灯组类型为1~12时，Bit1~Bit0用于表示红色发光单元，Bit3~Bit2用于表示黄色发光单元，Bit5~Bit4用于表示绿色发光单元，Bit7~ Bit6保留
2）当灯组类型为13~15时，Bit1~ Bit0用于表示禁止通行信号发光单元，Bit3~ Bit2用于表示过渡信号发光单元，Bit5~Bit4用于表示通行信号发光单元，Bit7~Bit6保留
2．具体取值
0：无灯
1：灭灯
2：亮灯
3：闪烁

	 * 
	 * **/
	
	public LightVo parseCurrlightPlan(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer regionId, Integer roadId)
	{
		LightVo vo=new LightVo();
		
		// 路口Id
//////////		String roadId = ""+receiverSn;////roadIdMap.get(port);
 
		// 经度
		float longitude = in.readFloatLE();///4
 
		// 纬度
		float latitude = in.readFloatLE();////4
 
		
		// 海拔
		int altitude = in.readShortLE();////2 bytes
 
		
		int lightGroupNum  =in.readByte();  ////灯组总数量	1 byte
		int entranceNum    =in.readByte();//////信号灯控制路口的进口数量1 byte
		
		logger.info("========>lightGroupNum="+lightGroupNum+", entranceNum="+entranceNum);
		
	    Plan plan=new Plan();
		
	    plan.setRegionId(regionId);
	    plan.setRoadId(roadId);
 
		
	    plan.setLon(longitude);
	    plan.setLat(latitude);
	    plan.setAlt(altitude);
	    plan.setLightGroupNum(lightGroupNum);
	    plan.setEntranceNum(entranceNum);

	    
	    List<PlanLightGroup> groups=new ArrayList<>();
		
		
		for(int i=0;i<entranceNum;i++)
		{
			int entranceDirection=in.readShortLE();////进口方向 2bytes
			int entranceGroupNum =in.readByte();/////进口灯组数量	1 byte
			
			logger.info("======i="+i+",=====>entranceDirection="+entranceDirection+",  =>entranceGroupNum="+entranceGroupNum);
			
			PlanLightGroup group=new PlanLightGroup();
			
			group.setEntranceDirection(entranceDirection);
			group.setEntranceGroupNum(entranceGroupNum);
			
			List<PlanColorInfo>  colorInfos=new ArrayList<>();
			
			
			for(int j=0;j<entranceGroupNum;j++)
			{
				int groupNo  =in.readByte();        /////灯组编号
				int groupType=in.readByte();        /////灯组类型
				int step     =in.readByte();        /////色步数  
				
				logger.info("===j="+j+"===>groupNo="+groupNo+", groupType="+groupType+", step="+step);
				
				
				PlanColorInfo colorInfo=new PlanColorInfo();
				colorInfo.setGroupNo(groupNo);
				colorInfo.setGroupType(groupType);
				colorInfo.setStep(step);
				
				List<PlanColorStep> colorSteps=new ArrayList<>();
				
		 
				for(int k=0;k<step;k++)
				{
					byte color	 =in.readByte();    /////灯色
					
					String s_color=SpatUtils.byteToBit(color);///// 0000,0000
					

//					
//					////////////////////////////////////
//					
					String  bit1bit0=s_color.substring(6,8);  ////Bit1~Bit0用于表示红色发光单元
					String  bit3bit2=s_color.substring(4,6);  ////Bit3~Bit2用于表示黄色发光单元
					String  bit5bit4=s_color.substring(2,4);  ////Bit5~Bit4用于表示绿色发光单元
					String  bit7bit6=s_color.substring(0,2);  ////resv
					
					
					int real_color=0;
					
					
					if(bit1bit0.equals("10"))  ///////---red
					{
						real_color=3;
					}
					else if(bit1bit0.equals("11")) ////---红闪
					{
						real_color=2;
					}
					else if(bit3bit2.equals("10"))  /////yellow
					{
						real_color=7;
					}
					else if(bit3bit2.equals("11"))  ////-----黄闪
					{
						real_color=8;
					}
					else if(bit5bit4.equals("10"))///---green
					{
						real_color=5;
					}
					else if(bit5bit4.equals("11"))  //////-------绿闪
					{
						real_color=6;
					}
					
					int colorTime=in.readShortLE() & 0xffff ;/////时间长度, 2byte  单位为s
					
					System.out.printf("========>k=%d, color=%x colorTime=%d \n",k, color, colorTime);
					logger.info("===k="+k+"==>s_color="+s_color+", colorTime="+colorTime);
					
					
					PlanColorStep colorStep=new PlanColorStep();
					
					colorStep.setColor(real_color);
					colorStep.setTotal(colorTime);
 
					
					colorSteps.add(colorStep);
					
					
					/////LinkedHashMap<String, Integer> map_plan=LoginInfo.map_currPlans.computeIfAbsent(roadId, x -> new LinkedHashMap<String, Integer>());
					
					///// 进口方向:灯组编号:灯组类型:灯色  ====>配时时间
					/////map_plan.put(entranceDirection+":"+groupNo+":"+groupType+":"+color, colorTime);

				}
				
				
				colorInfo.setColorSteps(colorSteps);
				colorInfos.add(colorInfo);

			}
			
			group.setColorInfos(colorInfos);
			
			groups.add(group);
			
		}
		
		plan.setGroups(groups);
		
		////////////////////////////////
		///////LoginInfo.map_currPlans=parsePlanTimes(  roadId,   entranceColor);
		////////////////////////////////
 
		vo.setMsgType("currPLan");
		vo.setRecvObj(plan);
		
		logger.info("======>[parseCurrlightPlan]road=["+roadId+"], vo="+JSON.toJSONString(vo));
		
		return vo;
	}
	
	
	
//	public JSONObject parseCurrlightPlan_old(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer receiverSn)
//	{
//		JSONObject data=new JSONObject();
// 
//		
//		
//		// 路口Id
//		String roadId = ""+receiverSn;////roadIdMap.get(port);
//		
//		
//		// 经度
//		float longitude = in.readFloatLE();///4
//		data.put("lon", longitude);
//		// 纬度
//		float latitude = in.readFloatLE();////4
//		data.put("lat", latitude);
//		
//		// 海拔
//		int altitude = in.readShortLE();////2 bytes
//		data.put("alt", altitude);
//		
//		int lightGroupTotal=in.readByte();  ////灯组总数量	1 byte
//		int entrance       =in.readByte();//////信号灯控制路口的进口数量1 byte
//		
//		logger.info("========>lightGroupTotal="+lightGroupTotal+", entrance="+entrance);
//		
//	    EntranceColor entranceColor=new EntranceColor();
//		
//		entranceColor.setRoadId(Integer.parseInt(roadId));
// 
//		
//		entranceColor.setLon(longitude);
//		entranceColor.setLat(latitude);
//		entranceColor.setAlt(altitude);
//		entranceColor.setEntranceNum(entrance);
//
//		List<EntranceLightGroup> groups=new ArrayList<>();
//		
//		
//		for(int i=0;i<entrance;i++)
//		{
//			int entranceDirection=in.readShortLE();////进口方向 2bytes
//			int entranceGroupNum =in.readByte();/////进口灯组数量	1 byte
//			
//			logger.info("======i="+i+",=====>entranceDirection="+entranceDirection+",  =>entranceGroupNum="+entranceGroupNum);
//			
//			EntranceLightGroup group=new EntranceLightGroup();
//			
//			group.setEntranceDirection(entranceDirection);
//			group.setEntranceGroupNum(entranceGroupNum);
//			
//			List<LightGroupColor> groupColors=new ArrayList<>();
//			
//			
//			for(int j=0;j<entranceGroupNum;j++)
//			{
//				int groupNo  =in.readByte();        /////灯组编号
//				int groupType=in.readByte();        /////灯组类型
//				int step     =in.readByte();        /////色步数  
//				
//				logger.info("===j="+j+"===>step="+step);
//		 
//				for(int k=0;k<step;k++)
//				{
//					byte color	 =in.readByte();    /////灯色
//					
//					String s_color=SpatUtils.byteToBit(color);///// 0000,0000
//					
//					System.out.printf("========>k=%d, color=%x  \n",k, color);
//					logger.info("===k="+k+"==>s_color="+s_color);
////					
////					////////////////////////////////////
////					
//					String  bit1bit0=s_color.substring(6,8);  ////Bit1~Bit0用于表示红色发光单元
//					String  bit3bit2=s_color.substring(4,6);  ////Bit3~Bit2用于表示黄色发光单元
//					String  bit5bit4=s_color.substring(2,4);  ////Bit5~Bit4用于表示绿色发光单元
//					String  bit7bit6=s_color.substring(0,2);  ////resv
//					
//					
//					int real_color=0;
//					
//					
//					if(bit1bit0.equals("10"))  ///////---red
//					{
//						real_color=3;
//					}
//					else if(bit1bit0.equals("11")) ////---红闪
//					{
//						real_color=2;
//					}
//					else if(bit3bit2.equals("10"))  /////yellow
//					{
//						real_color=7;
//					}
//					else if(bit3bit2.equals("11"))  ////-----黄闪
//					{
//						real_color=8;
//					}
//					else if(bit5bit4.equals("10"))///---green
//					{
//						real_color=5;
//					}
//					else if(bit5bit4.equals("11"))  //////-------绿闪
//					{
//						real_color=6;
//					}
//					
//					int colorTime=in.readShortLE();/////时间长度, 2byte  单位为s
//					
//					
//					LightGroupColor l_color=new LightGroupColor();
//					l_color.setGroupNo(groupNo);
//					l_color.setGroupType(groupType);
//					l_color.setColor(real_color);
//					l_color.setRemainTime(colorTime);
//					
//					groupColors.add(l_color);
//					
//					
//					/////LinkedHashMap<String, Integer> map_plan=LoginInfo.map_currPlans.computeIfAbsent(roadId, x -> new LinkedHashMap<String, Integer>());
//					
//					///// 进口方向:灯组编号:灯组类型:灯色  ====>配时时间
//					/////map_plan.put(entranceDirection+":"+groupNo+":"+groupType+":"+color, colorTime);
//
//				}
//
//			}
//			
//			group.setGroupColors(groupColors);
//			
//			groups.add(group);
//			
//		}
//		
//		entranceColor.setGroups(groups);
//		
//		////////////////////////////////
//		LoginInfo.map_currPlans=parsePlanTimes(  roadId,   entranceColor);
//		////////////////////////////////
//		
//		
//		String jsonStr=JSON.toJSONString(entranceColor);
//		
//		data=JSONObject.parseObject(jsonStr);
//		
//		data.put("msgType", "currPLan");
//		
//		logger.info("======>[CURRPLAN]road=["+roadId+"], data="+data.toJSONString());
//		
//		return data;
//	}
	
	/////////////////////////////////

	
	
	
	/////////////////////////////////
	
	
	/*******
	 * A.7.2　下一个周期信号方案色步信息查询应答
	 * 
	 * 
	 * 
	 * **/
	
	public LightVo parseNextlightPlan(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer regionId, Integer roadId)
	{
		LightVo vo=new LightVo();
 

		// 路口Id
///////////		String roadId = ""+receiverSn;////roadIdMap.get(port);
 
		// 经度
		float longitude = in.readFloatLE();///4
 
		// 纬度
		float latitude = in.readFloatLE();////4
 
		
		// 海拔
		int altitude = in.readShortLE();////2 bytes
 
		
		int lightGroupNum  =in.readByte();  ////灯组总数量	1 byte
		int entranceNum    =in.readByte();//////信号灯控制路口的进口数量1 byte
		
		logger.info("========>lightGroupNum="+lightGroupNum+", entranceNum="+entranceNum);
		
	    Plan plan=new Plan();
		
	    plan.setRegionId(regionId);
	    plan.setRoadId(roadId);
 
		
	    plan.setLon(longitude);
	    plan.setLat(latitude);
	    plan.setAlt(altitude);
	    plan.setLightGroupNum(lightGroupNum);
	    plan.setEntranceNum(entranceNum);

	    
	    List<PlanLightGroup> groups=new ArrayList<>();
		
		
		for(int i=0;i<entranceNum;i++)
		{
			int entranceDirection=in.readShortLE();////进口方向 2bytes
			int entranceGroupNum =in.readByte();   /////进口灯组数量	1 byte
			
			logger.info("======i="+i+",=====>entranceDirection="+entranceDirection+",  =>entranceGroupNum="+entranceGroupNum);
			
			PlanLightGroup group=new PlanLightGroup();
			
			group.setEntranceDirection(entranceDirection);
			group.setEntranceGroupNum(entranceGroupNum);
			
			List<PlanColorInfo>  colorInfos=new ArrayList<>();/////灯色信息
			
			
			for(int j=0;j<entranceGroupNum;j++)
			{
				int groupNo  =in.readByte();        /////灯组编号
				int groupType=in.readByte();        /////灯组类型
				int step     =in.readByte();        /////色步数  
				
				logger.info("===j="+j+"===>groupNo="+groupNo+", groupType="+groupType+", step="+step);
				
				
				PlanColorInfo colorInfo=new PlanColorInfo();
				colorInfo.setGroupNo(groupNo);
				colorInfo.setGroupType(groupType);
				colorInfo.setStep(step);
				
				List<PlanColorStep> colorSteps=new ArrayList<>();/////色步时间序列
				
		 
				for(int k=0;k<step;k++)
				{
					byte color	 =in.readByte();    /////灯色
					
					String s_color=SpatUtils.byteToBit(color);///// 0000,0000
					
					System.out.printf("========>k=%d, color=%x  \n",k, color);
					logger.info("===k="+k+"==>s_color="+s_color);
//					
//					////////////////////////////////////
//					
					String  bit1bit0=s_color.substring(6,8);  ////Bit1~Bit0用于表示红色发光单元
					String  bit3bit2=s_color.substring(4,6);  ////Bit3~Bit2用于表示黄色发光单元
					String  bit5bit4=s_color.substring(2,4);  ////Bit5~Bit4用于表示绿色发光单元
					String  bit7bit6=s_color.substring(0,2);  ////resv
					
					
					int real_color=0;
					
					
					if(bit1bit0.equals("10"))  ///////---red
					{
						real_color=3;
					}
					else if(bit1bit0.equals("11")) ////---红闪
					{
						real_color=2;
					}
					else if(bit3bit2.equals("10"))  /////yellow
					{
						real_color=7;
					}
					else if(bit3bit2.equals("11"))  ////-----黄闪
					{
						real_color=8;
					}
					else if(bit5bit4.equals("10"))///---green
					{
						real_color=5;
					}
					else if(bit5bit4.equals("11"))  //////-------绿闪
					{
						real_color=6;
					}
					
					int colorTime=in.readShortLE();/////时间长度, 2byte  单位为s
					
					
					PlanColorStep colorStep=new PlanColorStep();
					
					colorStep.setColor(real_color);
					colorStep.setTotal(colorTime);
 
					
					colorSteps.add(colorStep);
					
					
					/////LinkedHashMap<String, Integer> map_plan=LoginInfo.map_currPlans.computeIfAbsent(roadId, x -> new LinkedHashMap<String, Integer>());
					
					///// 进口方向:灯组编号:灯组类型:灯色  ====>配时时间
					/////map_plan.put(entranceDirection+":"+groupNo+":"+groupType+":"+color, colorTime);

				}
				
				
				colorInfo.setColorSteps(colorSteps);
				colorInfos.add(colorInfo);

			}
			
			group.setColorInfos(colorInfos);
			
			groups.add(group);
			
		}
		
		plan.setGroups(groups);
		///////////////////////////////////
 
		vo.setMsgType("nextPLan");
		vo.setRecvObj(plan);
		
		logger.info("======>[NEXTPLAN]road=["+roadId+"], vo="+JSON.toJSONString(vo));
		
		return vo;
	}
	
	
	
//	public JSONObject parseNextlightPlan_old(ChannelHandlerContext ctx, ByteBuf in, long UTCTime, int type, short operationObject, Integer receiverSn)
//	{
//		JSONObject data=new JSONObject();
// 
//		
//		
//		// 路口Id
//		String roadId =""+receiverSn; ////roadIdMap.get(port);
//		
//		
//		// 经度
//		float longitude = in.readFloatLE();///4
//		data.put("lon", longitude);
//		// 纬度
//		float latitude = in.readFloatLE();////4
//		data.put("lat", latitude);
//		
//		// 海拔
//		int altitude = in.readShortLE();////2 bytes
//		data.put("alt", altitude);
//		
//		int lightGroupTotal=in.readByte();  ////灯组总数量	1 byte
//		int entrance       =in.readByte();//////信号灯控制路口的进口数量1 byte
//		
//		logger.info("====lightGroupTotal="+lightGroupTotal+"==>entrance="+entrance);
//		
//	    EntranceColor entranceColor=new EntranceColor();
//		
//		entranceColor.setRoadId(Integer.parseInt(roadId));
// 
//		
//		entranceColor.setLon(longitude);
//		entranceColor.setLat(latitude);
//		entranceColor.setAlt(altitude);
//		entranceColor.setEntranceNum(entrance);
//
//		List<EntranceLightGroup> groups=new ArrayList<>();
//		
//		
//		for(int i=0;i<entrance;i++)
//		{
//			int entranceDirection=in.readShortLE();////进口方向 2bytes
//			int entranceGroupNum =in.readByte();/////进口灯组数量	1 byte
//			
//			logger.info("=============entranceDirection="+entranceDirection+"==>entranceGroupNum="+entranceGroupNum);
//			
//			EntranceLightGroup group=new EntranceLightGroup();
//			
//			group.setEntranceDirection(entranceDirection);
//			group.setEntranceGroupNum(entranceGroupNum);
//			
//			List<LightGroupColor> groupColors=new ArrayList<>();
//			
//			
//			for(int j=0;j<entranceGroupNum;j++)
//			{
//				int groupNo  =in.readByte();        /////灯组编号
//				int groupType=in.readByte();        /////灯组类型
//				int step     =in.readByte();        /////色步数  
//				
//				logger.info("======>step="+step);
//		 
//				for(int k=0;k<step;k++)
//				{
//					byte color	 =in.readByte();    /////灯色
//					
//					int colorTime=in.readShortLE();/////时间长度, 2byte  单位为s
//					
//					
//					LightGroupColor l_color=new LightGroupColor();
//					l_color.setGroupNo(groupNo);
//					l_color.setGroupType(groupType);
//					l_color.setColor(color);
//					l_color.setRemainTime(colorTime);
//					
//					groupColors.add(l_color);
//					
//					
//					////////LinkedHashMap<String, Integer> map_plan=LoginInfo.map_nextPlans.computeIfAbsent(roadId, x -> new LinkedHashMap<String, Integer>());
//					
//					///// 进口方向:灯组编号:灯组类型:灯色  ====>配时时间
//					/////////map_plan.put(entranceDirection+":"+groupNo+":"+groupType+":"+color, colorTime);
//
//				}
//
//			}
//			
//			group.setGroupColors(groupColors);
//			
//			groups.add(group);
//			
//		}
//		
//		entranceColor.setGroups(groups);
//		
//		///////////////////////////////////
//		LoginInfo.map_nextPlans=parsePlanTimes(  roadId,   entranceColor);
//		///////////////////////////////////
//		
//		String jsonStr=JSON.toJSONString(entranceColor);
//		
//		data=JSONObject.parseObject(jsonStr);
//		
//		data.put("msgType", "nextPLan");
//		
//		logger.info("======>[NEXTPLAN]road=["+roadId+"], data="+data.toJSONString());
//		
//		return data;
//	}
	
	
	
	/////////////----------------------------
    private void sendQuery(ChannelHandlerContext ctx) {
    	
    	while(true)
    	{
			if(!ctx.channel().isActive()){
				logger.info("Connection is closed, exit the loop.");
				break;
			}
    		
    		try {
    			
    			String [] arr_roads=ROADS.split("\\,");
    			
    			
    			for(String roadId: arr_roads)
    			{
    				LinkedHashMap<String, Integer> map_plan=null;////LoginInfo.map_currPlans.get(roadId);
    				
    				if(map_plan==null)  /////没有该路口的配时方案，则发起查询请求
    				{
    					
    					
    					////0x80	查询请求	发送查询消息
    					////0x0301  当前信号方案色步信息		描述信号机当前运行方案的灯色及时长
    					ByteBuf buf_plan=null;
    					
    					try {
							buf_plan=SpatUtils.buildQueryCurrPlan(ByteBufAllocator.DEFAULT, 
									CfgUtils.PlatRegionId, 
									(short)0x1000, (short)0xC0, 
									(short)0x2000, (short)0x0200, 
									(byte)0x80,  
									(short)0x0301  );
							
							logger.info("=============>send query curr_plan msg:"+ByteBufUtil.hexDump(buf_plan).toUpperCase() );
							
							ctx.writeAndFlush(
									new DatagramPacket( buf_plan,  new InetSocketAddress("255.255.255.255", CfgUtils.SERVER_PORT)  )
							 ).sync();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							logger.error("in send buf_plan", e);
						}
    					finally {
    						/////////////////////
        					ReferenceCountUtil.release(buf_plan);
    					}

    				}
    				
    			}
    			
    			
    			
				Thread.sleep(5000L);/////休眠5s
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("in sendQuery:"+e.getMessage(), e);
			}
    		
    	}
    	
    	
    }
	
	
}
