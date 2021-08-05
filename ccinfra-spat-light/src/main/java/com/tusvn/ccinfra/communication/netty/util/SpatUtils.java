package com.tusvn.ccinfra.communication.netty.util;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.druid.util.HexBin;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tusvn.ccinfra.communication.netty.domain.EntranceColor;
import com.tusvn.ccinfra.communication.netty.domain.EntranceLightGroup;
import com.tusvn.ccinfra.communication.netty.domain.LightGroupColor;
import com.tusvn.ccinfra.communication.netty.domain.Plan;
import com.tusvn.ccinfra.communication.netty.domain.PlanColorInfo;
import com.tusvn.ccinfra.communication.netty.domain.PlanColorStep;
import com.tusvn.ccinfra.communication.netty.domain.PlanLightGroup;
//import com.tusvn.ccinfra.communication.netty.handler.SpatHandler;
import com.tusvn.ccinfra.communication.netty.model.SpatData;
import com.tusvn.ccinfra.communication.netty.service.LoginInfo;
import com.tusvn.ccinfra.communication.netty.service.SpatService;
//import com.tusvn.ccinfra.config.ConfigProxy;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
//import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
//import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

public class SpatUtils {
	
	
	/////protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpatUtils.class);
	
	private static final Logger logger = LogManager.getLogger(SpatUtils.class);
	
	private final static byte FIXED_HEAD   = (byte)0xC0;   ////====>0xDB、 0xDC
	private final static byte SPECAIL_BYTE = (byte)0xDB;   ////====>0xDB、 0xDD

	public SpatUtils() {
		// TODO Auto-generated constructor stub
	}
	
	////b）	发送方标识：发送方唯一身份，长度7字节。编制规则为：行政区划代码+类型+编号，行政区划代码、类型、编号的取值应符合表2的规定
	////110115---大兴区
	public static ByteBuf getSendOrReceiverId(ByteBufAllocator allocator, int region, short type, short seq)
	{
		ByteBuf sendId = allocator.directBuffer(7);
		
		byte[] arr_region=int2ByteWithThree(region);
		
		sendId.writeBytes(arr_region);
		
		sendId.writeShortLE(type); ///////0010,0000 0000,0000
		
		sendId.writeShortLE(seq);  ///////0x01
		
		return sendId;
	}
	
	/********
	 * 
	 * 生成数据表
	 * 
	 * 数据表由链路码、发送方标识、接收方标识、时间戳、生存时间、协议版本、操作类型、对象标识、签名标记、保留、消息内容及签名证书构成，
	 * 
	 * operateType
	 *   0x80   查询请求	发送查询消息
	 *   0x83	查询应答	对查询请求的应答
	 *   0x87	信息广播	广播数据
	 * 
	 * 
	 * operateObj:
	 * 信号灯灯色状态/03	    0x0103	描述当前信号灯组的灯色和剩余时间
	 * 当前信号方案色步信息/01	0x0301	描述信号机当前运行方案的灯色及时长
	 * 
	 * **/
	public static ByteBuf generateReq(ByteBufAllocator allocator, 
			int region, 
			short sendType, short sendSeq, 
			short recvType, short recvSeq,
			byte  operateType,
			short operateObj
			)
	{
		ByteBuf buf = allocator.directBuffer();
		
		buf.writeShortLE(0x20);
		
		ByteBuf sendId=getSendOrReceiverId(  allocator,   region,   sendType,   sendSeq);
		buf.writeBytes(sendId);
		ReferenceCountUtil.safeRelease(sendId);
		
		ByteBuf recvId=getSendOrReceiverId(  allocator,   region,   recvType,   recvSeq);
		buf.writeBytes(recvId);
		ReferenceCountUtil.safeRelease(recvId);
		
		Long timestamp=1615428617000L;////System.currentTimeMillis();
		
		byte [] arr_timestamp=int2ByteWithSix(timestamp);
		buf.writeBytes(arr_timestamp);
		
		buf.writeByte(0x0F);    ////e）	生存时间（TTL）
		
		buf.writeByte(0x10);    ////f）	协议版本：协议的具体版本号，长度1字节，取值0x10；
		
		buf.writeByte(operateType);    /////g）	操作类型：数据表的查询、设置、应答等操作类型，长度1字节  0x80	查询请求	发送查询消息
		
		////////信号灯灯色状态/03	0x0103	描述当前信号灯组的灯色和剩余时间
		////////当前信号方案色步信息/01	0x0301	描述信号机当前运行方案的灯色及时长
		buf.writeShortLE(operateObj);  /////h）	对象标识：数据对象唯一编码，长度2字节。由对象分类编码（1字节）+对象名称编码（1字节）构成
		
		buf.writeByte(0x0);////i）	签名标记：标记数据表内容是否具有“签名证书”字段，长度1字节。按位取值，Bit0取值：0-无签名证书字段，1-有签名证书字段，Bit1~Bit7保留；
		
		/////j）	保留：长度3字节，用于数据表扩展的数据标识或内容定义；
		buf.writeCharSequence("000", CharsetUtil.UTF_8);
		
		return buf;
		
	}
	
	
	public static ByteBuf generateColorContent(ByteBufAllocator allocator)
	{
		ByteBuf buf = allocator.directBuffer();
		
		buf.writeFloatLE(116.4257052F);
		buf.writeFloatLE(39.75951F);
		buf.writeShortLE(2);
		buf.writeByte(1);
		
		buf.writeShortLE(1);
		buf.writeByte(1);
		
		buf.writeByte(8);
		buf.writeByte(2);
		buf.writeByte(0x20);
		buf.writeByte(46);
		
		
		return buf;
	}
	
	
	public static ByteBuf generateColorBroadst(ByteBufAllocator allocator, int region, short sendType, short sendSeq, 
			short recvType, short recvSeq,
			short operateObj, 
			ByteBuf content
			)
	{
		ByteBuf buf = allocator.directBuffer();
		
		buf.writeShortLE(0x20);
		
		ByteBuf sendId=getSendOrReceiverId(  allocator,   region,   sendType,   sendSeq);
		buf.writeBytes(sendId);
		ReferenceCountUtil.safeRelease(sendId);
		
		ByteBuf recvId=getSendOrReceiverId(  allocator,   region,   recvType,   recvSeq);
		buf.writeBytes(recvId);
		ReferenceCountUtil.safeRelease(recvId);
		
		Long timestamp=System.currentTimeMillis();
		
		byte [] arr_timestamp=int2ByteWithSix(timestamp);
		buf.writeBytes(arr_timestamp);
		
		buf.writeByte(0x0F);
		
		buf.writeByte(0x10);
		
		buf.writeByte(0x80);/////0x80	查询请求	发送查询消息
		
		////////信号灯灯色状态/03	0x0103	描述当前信号灯组的灯色和剩余时间
		////////当前信号方案色步信息/01	0x0301	描述信号机当前运行方案的灯色及时长
		buf.writeShortLE(operateObj);
		
		buf.writeByte(0x0);////i）	签名标记：标记数据表内容是否具有“签名证书”字段，长度1字节。按位取值，Bit0取值：0-无签名证书字段，1-有签名证书字段，Bit1~Bit7保留；
		
		/////j）	保留：长度3字节，用于数据表扩展的数据标识或内容定义；
		buf.writeCharSequence("000", CharsetUtil.UTF_8);
		
		buf.writeShortLE(content.readableBytes());
		buf.writeBytes(content);
		
		return buf;
	}
	
	
	public static ByteBuf msgSend() throws Exception
	{
		ByteBuf content=generateColorContent(ByteBufAllocator.DEFAULT);
		
		ByteBuf res=generateColorBroadst(ByteBufAllocator.DEFAULT, 110115, (short)0x20, (short)0x01, 
				(short)0x30, (short)0x02, 
				(short)0x0103, 
				content
		);
		
		
		ReferenceCountUtil.safeRelease(content);
				
		return res;
		
	}
	
    public static ByteBuf buildTestSend() throws Exception {
    	  ByteBuf src_buf=msgSend();
    	  
    	  byte[] src=new byte[src_buf.readableBytes()];
    	  
    	  src_buf.readBytes(src);
    	
    	
    	  if (src == null) throw new IllegalArgumentException("src not null");
          int length = src.length;
   
          //////////////////////////////
          ByteBuf tmp_buf = ByteBufAllocator.DEFAULT.directBuffer(length);
          // 数据负载
          tmp_buf.writeBytes(src);
          //校验
          int sign = sign(tmp_buf);
          tmp_buf.writeShort(sign);
          
          //转义  
          ByteBuf escape = escape(tmp_buf);
          
          /////////////////////////
          ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer(escape.readableBytes()+2);
          // 固定头 1 byte 0000 0010
          buf.writeByte(FIXED_HEAD);
          buf.writeBytes(escape);
          
          ////固定尾部
          buf.writeByte(FIXED_HEAD);
          
          
          ////////////////////////
          ////System.out.println("tmp_buf.ref_cnt="+tmp_buf.refCnt());
          ////ReferenceCountUtil.safeRelease(tmp_buf);
         ReferenceCountUtil.safeRelease(escape);
     
         ReferenceCountUtil.safeRelease(src_buf);
          
          return buf;
      }
	
	
	
	/*******
	 * 生成要发送的消息
	 * 
	 * 
	 * **/
    public static ByteBuf build(ByteBufAllocator allocator, byte[] src) throws Exception {
  	  if (src == null) throw new IllegalArgumentException("src not null");
        int length = src.length;
 
        //////////////////////////////
        ByteBuf tmp_buf = allocator.directBuffer(length);
        // 数据负载
        tmp_buf.writeBytes(src);
        
        /////logger.info("=============>before sign:"+ByteBufUtil.hexDump(tmp_buf).toUpperCase() );
        
        
        //校验
        int sign = sign(tmp_buf);
        
        //////logger.info("=============>sign="+sign);
        
        tmp_buf.writeShortLE(sign);
        
        //////////logger.info("=============>after sign:"+ByteBufUtil.hexDump(tmp_buf).toUpperCase() );
        
        //转义  
        ByteBuf escape = escape(tmp_buf);
        
        /////////logger.info("=============>after escape:"+ByteBufUtil.hexDump(tmp_buf).toUpperCase() );
        
        /////////////////////////
        ByteBuf buf = allocator.directBuffer(escape.readableBytes()+2);
        // 固定头 1 byte 0000 0010
        buf.writeByte(FIXED_HEAD);
        buf.writeBytes(escape);
        
        ////固定尾部
        buf.writeByte(FIXED_HEAD);
        
        
        ////////logger.info("=============>final buf:"+ByteBufUtil.hexDump(buf).toUpperCase() );
        
        
        ////////////////////////
        ////System.out.println("tmp_buf.ref_cnt="+tmp_buf.refCnt());
        ////ReferenceCountUtil.safeRelease(tmp_buf);
        ReferenceCountUtil.safeRelease(escape);
        
        return buf;
    }
    
    
    /////----2021-3-4 liaoxb 生成A.6.1　当前信号方案色步信息查询请求
    public static ByteBuf buildQueryCurrPlan(ByteBufAllocator allocator, 
    		int region, 
			short sendType, short sendSeq, 
			short recvType, short recvSeq,
			byte  operateType,
			short operateObj 
		) throws Exception 
    {
    	ByteBuf buf_req=generateReq(  allocator,  region,  sendType,   sendSeq, recvType,   recvSeq, operateType, operateObj );
    	
  	  	byte[] src=new byte[buf_req.readableBytes()];
	  
  	  	buf_req.readBytes(src);
  	
  	  	ByteBuf dst=build(allocator,  src);
    	
        ReferenceCountUtil.safeRelease(buf_req);
        
        return dst;
    }
    
    
    
    
    /**
     * 数据校验
     * @param buf
     * @return
     */
    private static int sign(ByteBuf buf) {
//    	byte[] dst = new byte[buf.readableBytes()-1];
//    	buf.getBytes(1, dst);
    	
    	byte[] dst = new byte[buf.readableBytes()];
    	buf.getBytes(0, dst);
    	
    	
    	return CRCUitl.CRC16_MODBUS(dst);
	}
    
	/**
     * 转义
     * 
     * c）	校验结束后应进行数据转义，数据表或校验码中某字节值为0xC0时使用0xDB、0xDC转义替换，为0xDB时使用0xDB、0xDD转义替换
     * 
     * @param buffer
     * @return 
     */
    private static ByteBuf escape(ByteBuf raw) {
        int len = raw.readableBytes();
    	ByteBuf buf = ByteBufAllocator.DEFAULT.directBuffer(len + 12);//假设最多有12个需要转义

    	while (len > 0) {
             byte b = raw.readByte();
             if (b == FIXED_HEAD) {
                 buf.writeByte(0xDB);
                 buf.writeByte(0xDC);
             } else if (b == SPECAIL_BYTE) {
                 buf.writeByte(0xDB);
                 buf.writeByte(0xDD);
             } else {
                 buf.writeByte(b);
             }
             len--;
        }
    	 
    	////System.out.println("1111raw.ref_cnt="+raw.refCnt());
        ReferenceCountUtil.safeRelease(raw);
        return buf;
	}
    
	
 
    /**
     * 一个int转4个字节的byte数组
     *
     * @param value
     * @return
     */
    public static byte[] int2Bytes(int value) {
        byte[] src = new byte[4];
        src[0] = (byte) ((value >> 24) & 0xFF);
        src[1] = (byte) ((value >> 16) & 0xFF);
        src[2] = (byte) ((value >> 8) & 0xFF);
        src[3] = (byte) (value & 0xFF);
        return src;
    }
    
    /**
     * 一个int转3个字节的byte数组
     *
     * @param value
     * @return
     */
    public static byte[] int2ByteWithThree(int value) {
        byte[] src = new byte[3];
 
        src[2] = (byte) ((value >> 16) & 0xFF);
        src[1] = (byte) ((value >> 8) & 0xFF);
        src[0] = (byte) (value & 0xFF);
        return src;
    }
    
    
    /**
     * 一个int转6个字节的byte数组
     *
     * @param value
     * @return
     */
    public static byte[] int2ByteWithSix(long value) {
        byte[] src = new byte[6];
 

        src[5] = (byte) ((value >> 40) & 0xFF);
        src[4] = (byte) ((value >> 32) & 0xFF);
        src[3] = (byte) ((value >> 24) & 0xFF);
        src[2] = (byte) ((value >> 16) & 0xFF);
        src[1] = (byte) ((value >> 8)  & 0xFF);
        src[0] = (byte) ( value        & 0xFF);
        return src;
    }
    
 
    //////3字节数组转整数
    public static int threeBytes2Int(byte[] bytes) { 
    	int off=0;
        int b2 = bytes[off]     & 0xFF; 
        int b1 = bytes[off + 1] & 0xFF; 
        int b0 = bytes[off + 2] & 0xFF; 
        ////int b3 = bytes[off + 3] & 0xFF; 
//        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3; 
        
        return (b0 << 16) | (b1 << 8) | (b2)  ; 
 
      }
    
    ////-----------2字节数组转整数
    public static int twoBytes2Int(byte[] bytes) { 
    	int off=0;
        int b2 = bytes[off]     & 0xFF; 
        int b1 = bytes[off + 1] & 0xFF; 
        ////int b0 = bytes[off + 2] & 0xFF; 
        ////int b3 = bytes[off + 3] & 0xFF; 
//        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3; 
        
        return   (b1 << 8) | (b2)  ; 
      }
    
    
    //////6字节数组转Long
    public static Long sixBytes2Long(byte[] bytes) { 
    	byte [] tmp=new byte[8];
    	
    	System.arraycopy(bytes, 0, tmp, 0, bytes.length);
    	
    	tmp[6]=tmp[7]=0;
    	
    	
    	ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(8);
    	buf.writeBytes(tmp);
    	
    	Long data=buf.readLongLE();
    	
    	ReferenceCountUtil.safeRelease(buf);
    	 
        return data;
        
    }
    
    //////6字节---4字节数组转Integer
    public static Integer sixBytes2Int(byte[] bytes) { 
 
    	ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(bytes.length);
    	buf.writeBytes(bytes);
    	
    	Integer data=buf.readIntLE();
    	
    	ReferenceCountUtil.safeRelease(buf);
    	 
        return data;
        
    }
    
    
	
	public static void daduan(int num)
	{
		ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(4);
		buf.writeInt(num);
		byte[] n = new byte[4];
		System.out.println(buf.readBytes(n));
		////System.out.println(Arrays.toString(n));
		
		
		ParseUtils.printByteArray("daduan", n);
		
		ReferenceCountUtil.safeRelease(buf);
	}
	
	public static void xiaoduan(int num)
	{
		ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(4);
		buf.writeLongLE(num);
		byte[] n = new byte[4];
		System.out.println(buf.readBytes(n));
		/////System.out.println(Arrays.toString(n));
		
		ParseUtils.printByteArray("xiaoduan", n);
		
		ReferenceCountUtil.safeRelease(buf);
	}
	
	public static void daduanWithLong(long num)
	{
		ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(8);
		buf.writeLong(num);
		byte[] n = new byte[8];
		System.out.println(buf.readBytes(n));
		////System.out.println(Arrays.toString(n));
		
		
		ParseUtils.printByteArray("daduan", n);
		
		ReferenceCountUtil.safeRelease(buf);
	}
	
	public static void xiaoduanWithLong(long num)
	{
		ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(8);
		buf.writeLongLE(num);
		byte[] n = new byte[8];
		System.out.println(buf.readBytes(n));
		/////System.out.println(Arrays.toString(n));
		
		ParseUtils.printByteArray("xiaoduan", n);
		
		
		ReferenceCountUtil.safeRelease(buf);
	}
	
	////b）	发送方标识：发送方唯一身份，长度7字节。编制规则为：行政区划代码3byte +类型2 byte+编号 2byte
	public static int getRegionId(byte [] receiverId)
	{
		
		ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(receiverId.length);
		
		buf.writeBytes(receiverId);
		
		byte [] dst=new byte[3];
		
		buf.readBytes(dst);
		
		int region=threeBytes2Int(dst);
		
		ReferenceCountUtil.safeRelease(buf);
		
		return region;
	
	}
	
	
	/** 
     * 把byte转为字符串的bit 
     */  
    public static String byteToBit(byte b) {  
        return ""  
                + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)  
                + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)  
                + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)  
                + (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1);  
    }
    
    
    public static byte[] getSendBytes() throws Exception
    {
		 ByteBuf test_buf=buildTestSend();
		 byte[] dst=new byte[test_buf.readableBytes()];		
		 test_buf.readBytes(dst);
		 
		 ReferenceCountUtil.safeRelease(test_buf);
		 
		 return dst;
    }
    
    
 
    /** 
     * 编码 
     * @param bstr 
     * @return String 
     */ 
     public static String encodeBase64(byte[] bstr){  
    	 return new sun.misc.BASE64Encoder().encode(bstr);  
     }  
      
     /** 
     * 解码 
     * @param str 
     * @return string 
     */ 
     public static byte[] decodeBase64(String str){
     byte[] bt = null;  
     try {  
       sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();  
       bt = decoder.decodeBuffer( str );  
     } catch (Exception e) {  
       e.printStackTrace();  
       logger.error("in decodeBase64" ,e);
     }  
      
       return bt;  
     } 
     
     
     /** 
      * 编码 
      * @param bstr 
      * @return String 
      */ 
      public static String encode(byte[] bstr){  
    	  java.util.Base64.Encoder encoder=java.util.Base64.getEncoder();
    	  
    	  String res=encoder.encodeToString(bstr);
    	  
    	  return res;
    	  
      }  
       
      /** 
      * 解码 
      * @param str 
      * @return string 
      */ 
	public static byte[] decode(String str) {
		byte[] bt = null;
		try {
			java.util.Base64.Decoder decoder = java.util.Base64.getDecoder();

			bt = decoder.decode(str);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("in decode", e);
		}

		return bt;
	}
      
    /****
     * 从发送者标识中截取数据
     * 
     * 
     * */
	public static int getDeviceSn(byte [] sender)
	{
		/////String [] data=new String[3];
		
		
    	ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(sender.length);
    	buf.writeBytes(sender);
    	
    	////行政区域代码3   类型2  编号2
    	byte[] b_region=new byte[3];
    	buf.readBytes(b_region);
    	
    	Integer regionId=SpatUtils.threeBytes2Int(b_region);
    	
    	short sendType=buf.readShortLE();
    	
    	
    	byte[] b_sn=new byte[2];
    	buf.readBytes(b_sn);
    	int sn=twoBytes2Int(b_sn);
    	////short sn=buf.readUnsignedByte();
    	
    	logger.info("a1="+regionId+",sendType="+sendType+", sn="+sn);
    	
//    	data[0]=""+regionId;
//    	data[1]=""+sendType;
//    	data[2]=""+sn;
    	
    	ReferenceCountUtil.release(buf);
    	 
        return sn;
	}
	
	public static short convertOperatObj(byte [] arr_operation)
	{
		ByteBuf buf = Unpooled.buffer(arr_operation.length);
		
		buf.writeBytes(arr_operation);
		
		short operateObj=buf.readShortLE();
		
		
		ReferenceCountUtil.release(buf);
		
		return operateObj;
		
	}
	
	public static JSONObject test_parse_bin()
	{
		////String line="C0040023AE010010FFFF23AE01002003002C91A36000003C1083030100000000590000000000000000000000040000040104165E0204165E0307165E0808253A5A0004050225130601253A0701253A0C08165EB400040904165E0A04165E0B07165E1008161B0E01040408165E0D0216420E01161B0F01161B7536C0";
		
		////String line="C0000024A6050010030024A6050020FFFFBE7DA7600000001087030100000000590000000000000000000000040000040104253302042533030725330808163B5A00040502163B0601163B0701163B0C082533B40004090425330A0425330B072533100816590E0104040825330D0216880E0116590F0116593343C0"  ;
		
		
		////String line="C0000024A6050010030024A6050020FFFFA480A7600000001087030100000000590000000000000000000000040000040104160E0204160E0307160E080816475A00040502164706011647070116470C08160EB400040904160E0A04160E0B07160E100825060E01040408160E0D0225060E0125060F01250650D5C0";
		
		////String line="C0000024A6050010110024A6050020FFFF334FCB600000001087030100000000590000000000000000000000040000040109190302021653030116070808166D5A00040504190306041903070416530C081607B40005090216530A0116070B0116070F0919031008167C0E0103040816070D0419030E0419039A6BC0";
		
		String line="C0000024A6050010110024A6050020FFFF8DADD0600000001087030100000000590000000000000000000000040000040109190102021651030116050808166B5A00040504190106041901070416510C081605B40005090216510A0116050B0116050F0919011008167A0E0103040816050D0419010E041901E231C0";
		
		
		byte[] decode = com.alibaba.druid.util.HexBin.decode(line);
		
		String codeStr=encode(decode);
		System.out.println("====>codeStr="+codeStr);
		
		
		
		ByteBuf in = Unpooled.buffer(decode.length);
		in.writeBytes(decode);
		
		// 获取协议头一位
		int head = in.getByte(0) & 0xFF;
		
		// 获取协议头最后一位
		int tail = in.getByte(in.readableBytes() - 1) & 0xFF;
		
		// 判断头和尾是不是0xc0
		if (!(192 == head && 192 == tail)) {
			
			logger.info("===============>head="+head+", tail="+tail);
			
			return null;
		}
		////LogWriter.printLog(logger, "SpatHandler -> hex16: {}", in, ConfigProxy.getBooleanOrDefault("printSpatLog", false));
		
		
		ParseUtils.printByteArray("dst", decode);
		

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
		
		
		////////////////////////////////////
		Integer regionId=SpatUtils.threeBytes2Int(sendId);
		logger.info("send.regionId="+regionId);
		
		
		////得到发送者的设备或平台唯一编号
		int senderSn=SpatUtils.getDeviceSn(sendId);   /////////------------------这里要转为路口id
		logger.info("====color===>senderSn="+senderSn);
		////////////////////////////////////////
		
		
		// 接收id
		byte[] receiveId = new byte[7];
		in.readBytes(receiveId);
		
		ParseUtils.printByteArray("receiveId", receiveId);
		
		
		Integer recv_regionId=SpatUtils.threeBytes2Int(receiveId);
		logger.info("recv.regionId="+regionId);
		
		
		int receiverSn=SpatUtils.getDeviceSn(receiveId);
		logger.info("====color===>receiverSn="+receiverSn);
		////////////////////////////////////////

		// UTC 时间 6
		byte[] arr_TUCTime =  new byte[6];
		in.readBytes(arr_TUCTime);
		
		ParseUtils.printByteArray("arr_TUCTime", arr_TUCTime);
		
		Long utcTime=SpatUtils.sixBytes2Long(arr_TUCTime);
		logger.info("utcTime="+utcTime);
		
		// TTL  1
		byte lifecycle = in.readByte();
		
		// 版本号（0x10）  1
		byte versionNumber = in.readByte();
		
		// 操作类型  1
		int operationType = in.readByte() & 0xFF;
		logger.info("operationType="+operationType);
		
		if(operationType!=0x83 && operationType!=0x87)
		{
			logger.info("==========>unknown operationType:"+operationType);
			return null;
		}
		
		
		// 对象标识   2
		byte[] arr_operation =  new byte[2];
		in.readBytes(arr_operation);
		
		short operationObject=convertOperatObj(arr_operation);
		logger.info("operationObject="+operationObject);
 
		
		System.out.printf("====color===>operationType=%x, operationObject=%x \n\n", operationType, operationObject );
		
		
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
		

		
		JSONObject obj=test_parseLightColor(   in,   utcTime,   operationType,   operationObject,   regionId,   senderSn,   System.currentTimeMillis());
		
		System.out.println("====>src jsonobj="+obj.toJSONString());
		
		
		return obj;
		
	}
    
	
	
	public static JSONObject test_parseLightColor( ByteBuf in, long UTCTime, int type, short operationObject, Integer regionId, Integer roadId, long currentTimeMillis)
	{
		JSONObject data=null;
		
		
		// 路口Id
//		String s_roadId = roadIdMap.get(port);  ///////////////////----------------根据端口得到路口id这里可能需要修改
//		
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
		int importDirectionNum = in.readByte(); /////1 byte
		
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
 
			logger.info("=====>directionI="+directionI);
			
			// 进口灯组数量
			int phaseNum = in.readByte();   //////1字节
			
			EntranceLightGroup group=new EntranceLightGroup();
			group.setEntranceDirection(directionI);
			group.setEntranceGroupNum(phaseNum);
			
			List<LightGroupColor> groupColors=new ArrayList<>();
 
			
			for (int j = 0; j < phaseNum; j++) {
 
				
				// 灯组编号id j
				int groupNo = in.readByte();    /////1字节
 
				
				// 灯组类型 j
				int groupType = in.readByte();   ////1字节

				
				logger.info("=====>groupNo="+groupNo);
				logger.info("=====>groupType="+groupType);
				
				
				// 灯色 j
				byte color   = in.readByte();    ////1字节
				String s_color=SpatUtils.byteToBit(color);///// 0000,0000
				
				System.out.printf("color=%x  \n", color);
				logger.info("=====>s_color="+s_color);
				
				////////////////////////////////////
				
				String  bit1bit0=s_color.substring(6,8);  ////Bit1~Bit0用于表示红色发光单元
				String  bit3bit2=s_color.substring(4,6);  ////Bit3~Bit2用于表示黄色发光单元
				String  bit5bit4=s_color.substring(2,4);  ////Bit5~Bit4用于表示绿色发光单元
				String  bit7bit6=s_color.substring(0,2);  ////resv
				
				
//				logger.info("====>bit1bit0="+bit1bit0);
//				logger.info("====>bit3bit2="+bit3bit2);
//				logger.info("====>bit5bit4="+bit5bit4);
//				logger.info("====>bit7bit6="+bit7bit6);
				
				////////////////////////////////////
				
				// 剩余时间 1 byte
				int remainTime = in.readByte() &0xff;
				
				
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
					}
					else if(bit1bit0.equals("11"))
					{
						groupColor.setColor(2);////红闪
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(3);
					}
 
					
					if(bit3bit2.equals("10"))
					{
						groupColor.setColor(7);////黄亮
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(2);
					}
					else if(bit3bit2.equals("11"))
					{
						groupColor.setColor(8);////黄闪
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(3);
					}
	 
 
					if(bit5bit4.equals("10"))
					{
						groupColor.setColor(5);////绿亮
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(2);
					}
					else if(bit5bit4.equals("11"))
					{
						groupColor.setColor(6);////绿闪
						groupColor.setRemainTime(remainTime);
						groupColor.setStatus(3);
					}
					////////logger.info("====>green color="+bit5bit4);
					
					
					groupColors.add(groupColor);

				}
				else /////当灯组类型为13~15时，Bit1~ Bit0用于表示禁止通行信号发光单元，Bit3~ Bit2用于表示过渡信号发光单元，Bit5~Bit4用于表示通行信号发光单元，Bit7~Bit6保留
				{
					logger.info("==================>skip groupType="+groupType);
					
				}
			}
			
			group.setGroupColors(groupColors);
			
			groups.add(group);
 
		}
		
		entranceColor.setGroups(groups);
		
		String jsonStr=JSON.toJSONString(entranceColor);
		
		data=JSONObject.parseObject(jsonStr);
		
		
		logger.info("======>[COLOR]road=["+roadId+"], data="+data.toJSONString());
		
		return data;
	}
	
	
	public static void test_parse_curr_plan()
	{
		////String line="C0040023AE010010FFFF23AE01002003002C91A36000003C1083030100000000590000000000000000000000040000040104165E0204165E0307165E0808253A5A0004050225130601253A0701253A0C08165EB400040904165E0A04165E0B07165E1008161B0E01040408165E0D0216420E01161B0F01161B7536C0";
		
		////String line="C0040023AE010010FFFF23AE0100200300687DA76000003C1083010300000000FE00000000000000000000001004000004010404166B00253300190400160400020404166B00253300190400160400030704166B002533001904001604000808032540003505001661005A0004050203251600190400168C00060103254500190400165D00070103254500190400165D000C0804166B00252E00350500160800B40004090404166B002533001904001604000A0404166B002533001904001604000B0704166B00253300190400160400100804161E002540003505001643000E0104040804166B00252E003505001608000D0204164D00251600190400163F000E0104161E00254500190400163F000F0104161E00254500190400163F006441C0"  ;
		
		String line="C0040023AE010010FFFF23AE0100200300A480A76000003C1083010300000000FE00000000000000000000001004000004010404165F00253100190400160400020404165F00253100190400160400030704165F00253100190400160400080803253600350500165D005A0004050203251400190400168000060103253B00190400165900070103253B001904001659000C0804165F00252C00350500160800B40004090404165F002531001904001604000A0404165F002531001904001604000B0704165F00253100190400160400100804161C002536003505001641000E0104040804165F00252C003505001608000D0204164300251400190400163D000E0104161C00253B00190400163D000F0104161C00253B00190400163D00B5ABC0";
		
		
		byte[] decode = com.alibaba.druid.util.HexBin.decode(line);
		
		String codeStr=encode(decode);
		System.out.println("====>codeStr="+codeStr);
		
		
		
		ByteBuf in = Unpooled.buffer(decode.length);
		in.writeBytes(decode);
		
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
		
		
		ParseUtils.printByteArray("dst", decode);
		

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
		
		
		////////////////////////////////////
		Integer regionId=SpatUtils.threeBytes2Int(sendId);
		logger.info("send.regionId="+regionId);
		
		
		////得到发送者的设备或平台唯一编号
		int senderSn=SpatUtils.getDeviceSn(sendId);   /////////------------------这里要转为路口id
		logger.info("=======>senderSn="+senderSn);
		////////////////////////////////////////
		
		
		// 接收id
		byte[] receiveId = new byte[7];
		in.readBytes(receiveId);
		
		ParseUtils.printByteArray("receiveId", receiveId);
		
		
		Integer recv_regionId=SpatUtils.threeBytes2Int(receiveId);
		logger.info("recv.regionId="+regionId);
		
		
		int receiverSn=SpatUtils.getDeviceSn(receiveId);
		logger.info("=======>receiverSn="+receiverSn);
		////////////////////////////////////////

		// UTC 时间 6
		byte[] arr_TUCTime =  new byte[6];
		in.readBytes(arr_TUCTime);
		
		ParseUtils.printByteArray("arr_TUCTime", arr_TUCTime);
		
		Long utcTime=SpatUtils.sixBytes2Long(arr_TUCTime);
		logger.info("utcTime="+utcTime);
		
		// TTL  1
		byte lifecycle = in.readByte();
		
		// 版本号（0x10）  1
		byte versionNumber = in.readByte();
		
		// 操作类型  1
		int operationType = in.readByte() & 0xFF;
		logger.info("operationType="+operationType);
		
		if(operationType!=0x83 && operationType!=0x87)
		{
			logger.info("==========>unknown operationType:"+operationType);
			return ;
		}
		
		
		// 对象标识   2
		byte[] arr_operation =  new byte[2];
		in.readBytes(arr_operation);
		
		short operationObject=convertOperatObj(arr_operation);
		logger.info("operationObject="+operationObject);
		logger.info("aaaaaaaaaaaaaaa,0x0301="+0x0301);
		
		System.out.printf("=====>operationType=%x, operationObject=%x\n\n",operationType, operationObject);
 
		
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
		

		
		JSONObject obj=test_parseCurrlightPlan(   in,   utcTime,   operationType,   operationObject,    receiverSn   );
		
		System.out.println("====>obj="+obj.toJSONString());
		
	}
	
	
	public static JSONObject test_parseCurrlightPlan(  ByteBuf in, long UTCTime, int type, short operationObject, Integer receiverSn)
	{
		JSONObject data=new JSONObject();
 
		
		
		// 路口Id
		String roadId = ""+receiverSn;////roadIdMap.get(port);
		
		
		// 经度
		float longitude = in.readFloatLE();///4
		data.put("lon", longitude);
		// 纬度
		float latitude = in.readFloatLE();////4
		data.put("lat", latitude);
		
		// 海拔
		int altitude = in.readShortLE();////2 bytes
		data.put("alt", altitude);
		
		int lightGroupNum  =in.readByte();  ////灯组总数量	1 byte
		int entranceNum    =in.readByte();//////信号灯控制路口的进口数量1 byte
		
		logger.info("========>lightGroupNum="+lightGroupNum+", entranceNum="+entranceNum);
		
	    Plan plan=new Plan();
		
	    plan.setRoadId(Integer.parseInt(roadId));
 
		
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
				
				////logger.info("===j="+j+"===>groupNo="+groupNo+", groupType="+groupType+", step="+step);
				
				
				PlanColorInfo colorInfo=new PlanColorInfo();
				colorInfo.setGroupNo(groupNo);
				colorInfo.setGroupType(groupType);
				colorInfo.setStep(step);
				
				List<PlanColorStep> colorSteps=new ArrayList<>();
				
		 
				for(int k=0;k<step;k++)
				{
					byte color	 =in.readByte();    /////灯色
					
					String s_color=SpatUtils.byteToBit(color);///// 0000,0000
					
//					System.out.printf("========>k=%d, color=%x  \n",k, color);
//					logger.info("===k="+k+"==>s_color="+s_color);
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
		
		plan.setGroups(groups);;
		
		////////////////////////////////
		///////LoginInfo.map_currPlans=parsePlanTimes(  roadId,   entranceColor);
		
		ConcurrentHashMap<String, Integer> map_plans=parsePlanTimes(  roadId,   plan);
		logger.info("===>map_plans="+JSON.toJSONString(map_plans));
		
		
		ConcurrentHashMap<String, Integer>  map_plans2=parsePlanTimesWithNoGroupNo(  roadId, map_plans);
		logger.info("===>map_plans2="+JSON.toJSONString(map_plans2));
		
		
		sort_map(map_plans);
		sort_map(map_plans2);
		
		
		LoginInfo.map_currPlans.putAll(map_plans);
		//////LoginInfo.map_currPlansWithNoGroupNo.putAll(map_plans2);
		
		LoginInfo.map_currPlansWithNoGroupNo.put(plan.getRoadId(), map_plans2);
		
		////////////////////////////////
		
		
		String jsonStr=JSON.toJSONString(plan);
		
		data=JSONObject.parseObject(jsonStr);
		
		data.put("msgType", "currPLan");
		
		logger.info("======>[CURRPLAN]road=["+roadId+"], data="+data.toJSONString());
		
		return data;
	}
	
	
//	public static JSONObject test_parseCurrlightPlan(  ByteBuf in, long UTCTime, int type, short operationObject, Integer receiverSn)
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
//		int lightGroupTotal=in.readByte() & 0xff;  ////灯组总数量	1 byte
//		int entrance       =in.readByte() & 0xff;//////信号灯控制路口的进口数量1 byte
//		
//		logger.info("======>lightGroupTotal="+lightGroupTotal);
//		logger.info("======>entrance="+entrance);
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
//			logger.info("======>i="+i+"====>entranceDirection="+entranceDirection);
//			logger.info("======>i="+i+",entranceGroupNum="+entranceGroupNum);
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
//				logger.info("===========>j="+j+"=groupNo="+groupNo+", groupType="+groupType+"===>step="+step);
//		 
//				for(int k=0;k<step;k++)
//				{
//					byte color	 =in.readByte();    /////灯色
//
//					//////////////////////////////////////
//					// 灯色 j
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
////					aaa
////					
////					logger.info("====>bit1bit0="+bit1bit0);
////					logger.info("====>bit3bit2="+bit3bit2);
////					logger.info("====>bit5bit4="+bit5bit4);
////					logger.info("====>bit7bit6="+bit7bit6);
//					//////////////////////////////////////////////////////////
//					
//					int colorTime=in.readShortLE();/////时间长度, 2byte  单位为s
//					logger.info("=======>k="+k+"=real_color="+real_color+"=>colorTime="+colorTime);
//					
//					LightGroupColor l_color=new LightGroupColor();
//					l_color.setGroupNo(groupNo);
//					l_color.setGroupType(groupType);
//					
//					
//					l_color.setColor(real_color);
//					
//					
//					
//					l_color.setRemainTime(colorTime);
//					
//					groupColors.add(l_color);
//					
//					
//					///////LinkedHashMap<String, Integer> map_plan=LoginInfo.map_currPlans.computeIfAbsent(roadId, x -> new LinkedHashMap<String, Integer>());
//					
//					///// 进口方向:灯组编号:灯组类型:灯色  ====>配时时间
//					///////map_plan.put(entranceDirection+":"+groupNo+":"+groupType+":"+color, colorTime);
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
//		///////////////////////////////////////////
//		ConcurrentHashMap<String, Integer> map_plan=SpatHandler.parsePlanTimes(  roadId,   entranceColor);
//		logger.info("======>[CURRPLAN]road=["+roadId+"], map_plan="+JSON.toJSONString(map_plan));
//		///////////////////////////////////////////
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
	
	
	
	
	
	
	public static void test_parse_next_plan()
	{
		////String line="C0040023AE010010FFFF23AE01002003002C91A36000003C1083030100000000590000000000000000000000040000040104165E0204165E0307165E0808253A5A0004050225130601253A0701253A0C08165EB400040904165E0A04165E0B07165E1008161B0E01040408165E0D0216420E01161B0F01161B7536C0";
		
		String line=""  ;
		
		byte[] decode = com.alibaba.druid.util.HexBin.decode(line);
		
		String codeStr=encode(decode);
		System.out.println("====>codeStr="+codeStr);
		
		
		
		ByteBuf in = Unpooled.buffer(decode.length);
		in.writeBytes(decode);
		
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
		
		
		ParseUtils.printByteArray("dst", decode);
		

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
		
		
		////////////////////////////////////
		Integer regionId=SpatUtils.threeBytes2Int(sendId);
		logger.info("send.regionId="+regionId);
		
		
		////得到发送者的设备或平台唯一编号
		int senderSn=SpatUtils.getDeviceSn(sendId);   /////////------------------这里要转为路口id
		logger.info("=======>senderSn="+senderSn);
		////////////////////////////////////////
		
		
		// 接收id
		byte[] receiveId = new byte[7];
		in.readBytes(receiveId);
		
		ParseUtils.printByteArray("receiveId", receiveId);
		
		
		int receiverSn=SpatUtils.getDeviceSn(receiveId);
		logger.info("=======>receiverSn="+receiverSn);
		////////////////////////////////////////

		// UTC 时间 6
		byte[] arr_TUCTime =  new byte[6];
		in.readBytes(arr_TUCTime);
		
		ParseUtils.printByteArray("arr_TUCTime", arr_TUCTime);
		
		Long utcTime=SpatUtils.sixBytes2Long(arr_TUCTime);
		logger.info("utcTime="+utcTime);
		
		// TTL  1
		byte lifecycle = in.readByte();
		
		// 版本号（0x10）  1
		byte versionNumber = in.readByte();
		
		// 操作类型  1
		int operationType = in.readByte() & 0xFF;
		logger.info("operationType="+operationType);
		
		System.out.printf("====>operationType=%x\n", operationType);
		
		if(operationType!=0x83 && operationType!=0x87)
		{
			logger.info("==========>unknown operationType:"+operationType);
			return ;
		}
		
		
		// 对象标识   2
		byte[] arr_operation =  new byte[2];
		in.readBytes(arr_operation);
		
		short operationObject=convertOperatObj(arr_operation);
		
		System.out.printf("====>operationObject=%x\n", operationObject);
		
		logger.info("operationObject="+operationObject);
		
		System.out.printf("=====>operationType=%x, operationObject=%x\n\n",operationType, operationObject);
 
		
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
		

		
		JSONObject obj=parseNextlightPlan(   in,   utcTime,   operationType,   operationObject,    receiverSn   );
		
		System.out.println("====>obj="+obj.toJSONString());
		
	}
	
	
	
	public static JSONObject parseNextlightPlan( ByteBuf in, long UTCTime, int type, short operationObject, Integer receiverSn)
	{
		JSONObject data=new JSONObject();
 

		// 路口Id
		String roadId = ""+receiverSn;////roadIdMap.get(port);
		
		
		// 经度
		float longitude = in.readFloatLE();///4
		data.put("lon", longitude);
		// 纬度
		float latitude = in.readFloatLE();////4
		data.put("lat", latitude);
		
		// 海拔
		int altitude = in.readShortLE();////2 bytes
		data.put("alt", altitude);
		
		int lightGroupNum  =in.readByte();  ////灯组总数量	1 byte
		int entranceNum    =in.readByte();//////信号灯控制路口的进口数量1 byte
		
		logger.info("========>lightGroupNum="+lightGroupNum+", entranceNum="+entranceNum);
		
	    Plan plan=new Plan();
		
	    plan.setRoadId(Integer.parseInt(roadId));
 
		
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
		
		String jsonStr=JSON.toJSONString(plan);
		
		data=JSONObject.parseObject(jsonStr);
		
		data.put("msgType", "nextPLan");
		
		logger.info("======>[NEXTPLAN]road=["+roadId+"], data="+data.toJSONString());
		
		return data;
	}
	
	
	/******
	 * 根据 EntranceColor 中消息求配时方案中的时长
	 * key:
	 * 路口id:inDirection:groupType:color:groupNo====>totalTime
	 * 
	 * **/
	public static ConcurrentHashMap<String, Integer>  parsePlanTimes(String roadId, Plan plan)
	{
		ConcurrentHashMap<String, Integer> map_plans=new ConcurrentHashMap<String, Integer>();
		
		List<PlanLightGroup> groups=plan.getGroups();
		
		if(groups!=null && groups.size()>0)
		{
			
			for(PlanLightGroup group: groups)
			{
				
				Integer entranceDirection=group.getEntranceDirection();
				
				List<PlanColorInfo>  colorInfos=group.getColorInfos();
				
				if(colorInfos!=null && colorInfos.size()>0)
				{
					
					for(PlanColorInfo colorInfo: colorInfos)
					{
						
						int groupNo  =colorInfo.getGroupNo();                     ////灯组编号
						
						int groupType=colorInfo.getGroupType();                   ////灯组类型 
					    int stepNum  =colorInfo.getStep();                        ////色步数
						List<PlanColorStep> colorSteps=colorInfo.getColorSteps();  ////色步时间序列
						
						if(colorSteps!=null && colorSteps.size()>0)
						{
							for(PlanColorStep step: colorSteps)
							{
								
								int color=step.getColor();
								int total=step.getTotal();
								
								
								if(color==3 || color==5 || color==7) /////等于3红  5绿  7黄时
								{
									/////路口id:inDirection:groupType:color:groupNo====>totalTime
									String key=roadId+":"+entranceDirection+":"+groupType+":"+color+":"+groupNo;
									
									Integer tmp_val=map_plans.get(key);
									
									if(tmp_val==null)
									{
										map_plans.put(key, total);
									}
									else
										map_plans.put(key, tmp_val+total);
								}
								else
								{
									int tmp_color=0;
									
									if(color==2)   //////红闪
										tmp_color=3;    ////红
									else if(color==6) /////绿闪
										tmp_color=5;  ////绿
									else if(color==8) ////黄闪
										tmp_color=7;   ////黄
										
									
									
									/////路口id:inDirection:groupType:color:groupNo====>totalTime
									String key=roadId+":"+entranceDirection+":"+groupType+":"+tmp_color+":"+groupNo;
									
									Integer tmp_val=map_plans.get(key);
									
									if(tmp_val==null)
									{
										map_plans.put(key, total);
									}
									else
										map_plans.put(key, tmp_val+total);
								}

								
							}
							
							
							
						}
						

 
						
					}
 
				}
 
				
			}
 
			
		}
		
		return map_plans;
		
	}
	
	////////----------/////路口id:inDirection:groupType:color:groupNo====>totalTime
	////////==============/////路口id:inDirection:groupType:color====>totalTime
	public static ConcurrentHashMap<String, Integer>  parsePlanTimesWithNoGroupNo(String roadId, ConcurrentHashMap<String, Integer> src_map_plans)
	{
		ConcurrentHashMap<String, Integer> dest_map_plans=new ConcurrentHashMap<String, Integer>();
		
		
		for(Map.Entry<String, Integer> entry: src_map_plans.entrySet())
		{
			
			String key =entry.getKey();
			
			Integer val=entry.getValue();
			
			
			String [] arr=key.split("\\:");
			
			String s_roadId=arr[0];
			String s_inDirection=arr[1];
			String s_groupType=arr[2];
			String s_color=arr[3];
			String s_groupNo=arr[4];
			
			String dest_key=s_roadId+":"+s_inDirection+":"+s_groupType+":"+s_color;
			
			
			dest_map_plans.put(dest_key, val);
			
			
		}
		
		
		return dest_map_plans;
		
	}
	
	
	public static void sort_map(ConcurrentHashMap<String, Integer>  map_currPlansWithNoGroupNo)
	{
		TreeMap<String, Integer > map=new TreeMap<>();
		
		
		map.putAll(map_currPlansWithNoGroupNo);
		
		System.out.println("=====1111111111111111111=>map="+JSON.toJSONString(map));
		
	}
	
	
	public static int getPid() {  
        //获取进程的PID 
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();   
        String name = runtime.getName(); // format: "pid@hostname"   
        try {   
            return Integer.parseInt(name.substring(0, name.indexOf('@')));   
        } catch (Exception e) {
        	e.printStackTrace();
        	logger.error("in getPid",  e);
            return -1;   
        }   
    }
	
	/*****
	 * System.getProperty("user.dir")  为获取用户当前工作目录
	 * 
	 * 
	 * ***/
	public static String writePid(String path, String serviceName, int pid)
	{
		String res=null;
		
		if(path!=null && !path.endsWith("/"))
			path=path+"/";
		
		/////data/ccinfra-spat-light/spat.pid
		String file=path+serviceName+".pid";
		
		try (FileWriter writer = new FileWriter(file)) {
			writer.append(""+pid);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("in writePid", e);
			
			res=""+e.getMessage();
		}
		
		return res;
	}
	
	

	public static void main(String[] args) throws Exception
	{
		// TODO Auto-generated method stub
		
		
		
//		 ByteBuf buf_dst=buildQueryCurrPlan(ByteBufAllocator.DEFAULT, 
//		    		110115, 
//		    		(short)0x1000, (short)0xC0, 
//		    		(short)0x2000, (short)0x0200, 
//					(byte)0x80,
//					(short)0x0301
//				);
//		 
//		 byte[] dst=new byte[buf_dst.readableBytes()];	
//		 buf_dst.readBytes(dst);
//		 ParseUtils.printByteArray("dst", dst);
		
		
//		ByteBuf src=generateReq(ByteBufAllocator.DEFAULT, 110115, (short)0x1000, (short)0xC0, 
//				(short)0x2000, (short)0x0200, 
//				(short)0x0103
//				);
//		byte[] dst=new byte[src.readableBytes()];		
//		src.readBytes(dst);
//		ParseUtils.printByteArray("dst", dst);
//		
//		ByteBuf buf=build(ByteBufAllocator.DEFAULT, dst);
//		byte[] dst2=new byte[buf.readableBytes()];		
//		buf.readBytes(dst2);
//		ParseUtils.printByteArray("dst2", dst2);

		/////////////////////////////
		
//		ByteBuf src=getSendOrReceiverId(ByteBufAllocator.DEFAULT, 110115, (short)0x20, (short)0x1);
//		byte[] dst=new byte[src.readableBytes()];		
//		src.readBytes(dst);
//		
//		ParseUtils.printByteArray("dst", dst);
//		
//		int id=getRegionId(dst);
//		System.out.println("=======>id="+id);
		
		
//		long num=1535980042858L;
//		
//		byte[] arr=int2ByteWithSix(num);
//		ParseUtils.printByteArray("arr", arr);
//		
//		Long n=sixBytes2Long(arr);
//		System.out.println("=======>n="+n);
		
		
		
//		
//		daduanWithLong(num);
//		
//		xiaoduanWithLong(num);
		
//		int num=110115;
//		
//		byte [] arr=int2ByteWithThree(num);
//		ParseUtils.printByteArray("arr", arr);
//		
//		
//		byte [] arr2=int2Bytes(num);
//		ParseUtils.printByteArray("arr2", arr2);
//		
//		
//		daduan(num);
//		
//		System.out.println();
//		
//		xiaoduan(num);
		
		///////////////////////////////////
		
//		ByteBuf buf=getSendOrReceiverId(ByteBufAllocator.DEFAULT,  110115, (short)0x20, (short)0x01);
//		
//		byte[] dst=new byte[buf.readableBytes()];
//		
//		buf.readBytes(dst);
//		
//		
//		ParseUtils.printByteArray("dst", dst);
//		
//		short sn=getDeviceSn(dst);
//		
//		System.out.println("====>sn="+sn);
		
		////////////////////////
	    ////
		
		
		////test_parse_curr_plan();
		
		JSONObject jsonObj=test_parse_bin();
		
		
//		System.out.println("LoginInfo.map_currPlansWithNoGroupNo="+JSON.toJSONString(LoginInfo.map_currPlansWithNoGroupNo));
//		
//		SpatData spData=SpatService.convertSpatData(jsonObj, LoginInfo.map_currPlansWithNoGroupNo.get(3));
//		
//		System.out.println("====>spData="+JSON.toJSONString(spData));
		
		////test_parse_next_plan();
		
//		int crc=0x3675;
		
    	
//    	ByteBuf buf = Unpooled.buffer(2);	
//    	////buf.writeShortLE(crc);           /////75 36  
//    	
//    	buf.writeShort(crc);                 /////36 75
//    	
//    	byte [] arr_crc=buf.array();
// 
//		
//		ParseUtils.printByteArray("bbbbbb2", arr_crc);
		
		
		
		////System.out.println("operObj="+0x0103);
		

	}

}
