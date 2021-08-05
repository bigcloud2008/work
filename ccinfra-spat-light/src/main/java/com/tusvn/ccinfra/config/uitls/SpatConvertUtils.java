package com.tusvn.ccinfra.config.uitls;

import java.util.HashMap;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.druid.util.HexBin;
 
import com.tusvn.ccinfra.communication.netty.model.PhaseState;
import com.tusvn.ccinfra.communication.netty.util.LogWriter;
import com.tusvn.ccinfra.communication.netty.util.ParseUtils;
 
import com.tusvn.ccinfra.config.ConfigProxy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;



/***********
 * 2021-4-21 liaoxb add 按53-2020规范新增spat的4个字段
 * 
 * 
 * 
 * ****/
public class SpatConvertUtils {
	
	private static final Logger logger = LogManager.getLogger(SpatConvertUtils.class);

	public SpatConvertUtils() {
		// TODO Auto-generated constructor stub
	}
	
	
	///////////////////////////------------------绿------》黄-------》红-------》绿
	/////--------------当前是绿灯
	/////--------------green_likelyEndTime,green_nextDuration,yellow_nextDuration,red_nextDuration单位是秒
	public static HashMap<Integer, PhaseState> calcLightTimeForCurrGreen(Long intersectionTimestamp, int green_likelyEndTime, 
			int green_nextDuration, int yellow_nextDuration, int red_nextDuration)
	{
		HashMap<Integer, PhaseState> res_times=new HashMap<>();
		
		///////////5----绿 
		PhaseState green_PhaseState=new  PhaseState(5, 0, green_likelyEndTime,  green_nextDuration);
		
		green_PhaseState.setStartUTCTime(intersectionTimestamp-(green_nextDuration-green_likelyEndTime)*1000);
		green_PhaseState.setLikelyEndUTCTime(intersectionTimestamp+green_likelyEndTime*1000);
		
		green_PhaseState.setNextStartUTCTime((long) (green_likelyEndTime+(yellow_nextDuration+red_nextDuration)*1000));
		green_PhaseState.setNextEndUTCTime(green_PhaseState.getNextStartUTCTime()+green_nextDuration*1000);
		
		
		////////////////////////////////////////
		//////////7--黄
		PhaseState yellow_PhaseState=new PhaseState(7, green_likelyEndTime, yellow_nextDuration,  yellow_nextDuration);
		
		yellow_PhaseState.setStartUTCTime(green_PhaseState.getLikelyEndUTCTime());
		yellow_PhaseState.setLikelyEndUTCTime(yellow_PhaseState.getStartUTCTime()+yellow_nextDuration*1000);
		
		yellow_PhaseState.setNextStartUTCTime((long) (yellow_PhaseState.getLikelyEndUTCTime()+(red_nextDuration+green_nextDuration)*1000));
		yellow_PhaseState.setNextEndUTCTime(yellow_PhaseState.getNextStartUTCTime()+yellow_nextDuration*1000);
		
		
		/////////3---红
		PhaseState red_PhaseState=new PhaseState(3, green_likelyEndTime+yellow_nextDuration, red_nextDuration,  red_nextDuration);
		
		red_PhaseState.setStartUTCTime(yellow_PhaseState.getLikelyEndUTCTime());
		red_PhaseState.setLikelyEndUTCTime(red_PhaseState.getStartUTCTime()+red_nextDuration*1000);
		
		red_PhaseState.setNextStartUTCTime((long) (red_PhaseState.getLikelyEndUTCTime()+(green_nextDuration+yellow_nextDuration)*1000));
		red_PhaseState.setNextEndUTCTime(red_PhaseState.getNextStartUTCTime()+red_nextDuration*1000);
		
		
		/////////////////////////////////////////////
		
		res_times.put(5, green_PhaseState);
		res_times.put(7, yellow_PhaseState);
		res_times.put(3, red_PhaseState);
		
		return res_times;
	}
	
	/////--------------当前是黄灯
	public static HashMap<Integer, PhaseState> calcLightTimeForCurrYellow(Long intersectionTimestamp, int yellow_likelyEndTime, 
			int green_nextDuration, int yellow_nextDuration, int red_nextDuration)
	{
		HashMap<Integer, PhaseState> res_times=new HashMap<>();
 
		////////////////////////////////////////
		//////////7--黄                ///    int light, int startTime, int likelyEndTime, int nextDuratio
		PhaseState yellow_PhaseState=new PhaseState(7, 0, yellow_likelyEndTime,  yellow_nextDuration);
		
		yellow_PhaseState.setStartUTCTime(intersectionTimestamp-(yellow_nextDuration-yellow_likelyEndTime)*1000);
		yellow_PhaseState.setLikelyEndUTCTime(intersectionTimestamp+yellow_likelyEndTime*1000);
		
		yellow_PhaseState.setNextStartUTCTime((long) (yellow_PhaseState.getLikelyEndUTCTime()+(red_nextDuration+green_nextDuration)*1000));
		yellow_PhaseState.setNextEndUTCTime(yellow_PhaseState.getNextStartUTCTime()+yellow_nextDuration*1000);
		
 
		
		/////////3---红
		PhaseState red_PhaseState=new PhaseState(3, yellow_likelyEndTime, red_nextDuration,  red_nextDuration);
		
		red_PhaseState.setStartUTCTime(yellow_PhaseState.getLikelyEndUTCTime());
		red_PhaseState.setLikelyEndUTCTime(red_PhaseState.getStartUTCTime()+red_nextDuration*1000);
		
		red_PhaseState.setNextStartUTCTime((long) (red_PhaseState.getLikelyEndUTCTime()+(green_nextDuration+yellow_nextDuration)*1000));
		red_PhaseState.setNextEndUTCTime(red_PhaseState.getNextStartUTCTime()+red_nextDuration*1000);
		
		
		///////////5----绿 
		PhaseState green_PhaseState=new  PhaseState(5, yellow_likelyEndTime+red_nextDuration, green_nextDuration,  green_nextDuration);
		
		green_PhaseState.setStartUTCTime(intersectionTimestamp+yellow_likelyEndTime*1000+red_nextDuration*1000);
		green_PhaseState.setLikelyEndUTCTime(green_PhaseState.getStartUTCTime()+green_nextDuration*1000);
		
		green_PhaseState.setNextStartUTCTime((long) (green_PhaseState.getLikelyEndUTCTime()+(yellow_nextDuration+red_nextDuration)*1000));
		green_PhaseState.setNextEndUTCTime(green_PhaseState.getNextStartUTCTime()+green_nextDuration*1000);
		
		
		
		/////////////////////////////////////////////
		
		res_times.put(5, green_PhaseState);
		res_times.put(7, yellow_PhaseState);
		res_times.put(3, red_PhaseState);
		
		return res_times;
	}
	
	/////--------------当前是红灯
	public static HashMap<Integer, PhaseState> calcLightTimeForCurrRed(Long intersectionTimestamp, int red_likelyEndTime, 
			int green_nextDuration, int yellow_nextDuration, int red_nextDuration)
	{
		HashMap<Integer, PhaseState> res_times=new HashMap<>();
 
		////////////////////////////////////////
		
		/////////3---红
		PhaseState red_PhaseState=new PhaseState(3, 0, red_likelyEndTime,  red_nextDuration);
		
		red_PhaseState.setStartUTCTime(intersectionTimestamp-(red_nextDuration-red_likelyEndTime)*1000);
		red_PhaseState.setLikelyEndUTCTime(intersectionTimestamp+red_likelyEndTime*1000);
		
		red_PhaseState.setNextStartUTCTime((long) (red_PhaseState.getLikelyEndUTCTime()+(green_nextDuration+yellow_nextDuration)*1000));
		red_PhaseState.setNextEndUTCTime(red_PhaseState.getNextStartUTCTime()+red_nextDuration*1000);
		
		
		///////////5----绿 
		PhaseState green_PhaseState=new PhaseState(5, red_likelyEndTime, green_nextDuration,  green_nextDuration);
		
		green_PhaseState.setStartUTCTime(red_PhaseState.getLikelyEndUTCTime());
		green_PhaseState.setLikelyEndUTCTime(green_PhaseState.getStartUTCTime()+green_nextDuration*1000);
		
		green_PhaseState.setNextStartUTCTime((long) (green_PhaseState.getLikelyEndUTCTime()+(yellow_nextDuration+red_nextDuration)*1000));
		green_PhaseState.setNextEndUTCTime(green_PhaseState.getNextStartUTCTime()+green_nextDuration*1000);
		

		
		//////////7--黄
		PhaseState yellow_PhaseState=new PhaseState(7, red_likelyEndTime+green_nextDuration, yellow_nextDuration,  yellow_nextDuration);
		
		yellow_PhaseState.setStartUTCTime(green_PhaseState.getLikelyEndUTCTime());
		yellow_PhaseState.setLikelyEndUTCTime(yellow_PhaseState.getStartUTCTime()+yellow_nextDuration*1000);
		
		yellow_PhaseState.setNextStartUTCTime((long) (yellow_PhaseState.getLikelyEndUTCTime()+(red_nextDuration+green_nextDuration)*1000));
		yellow_PhaseState.setNextEndUTCTime(yellow_PhaseState.getNextStartUTCTime()+yellow_nextDuration*1000);
		
 
		
		/////////////////////////////////////////////
		
		res_times.put(5, green_PhaseState);
		res_times.put(7, yellow_PhaseState);
		res_times.put(3, red_PhaseState);
		
		return res_times;
	}
	
	

	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
