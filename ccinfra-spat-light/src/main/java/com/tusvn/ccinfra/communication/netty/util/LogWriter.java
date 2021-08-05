package com.tusvn.ccinfra.communication.netty.util;

import com.tusvn.ccinfra.config.ConfigProxy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

/**
 * LogWriter
 * 
 * <p>
 * </p>
 * <p>
 * Create by xchao on 2018-11-09
 * </p>
 * 
 * @author xchao (xiongchao@tusvn.com)
 * @date 2018-11-09
 */
public final class LogWriter {

	/**
	 * 是否打印debug日志
	 * 
	 * @param logger
	 * @param content
	 */
	public static void printLog(org.slf4j.Logger logger, String content) {
		if (getPrintDebugLog()) {
			logger.info(content);
		}
	}

	private static boolean getPrintDebugLog() {
		return ConfigProxy.getBooleanOrDefault("printDebugLog", false);
	}

	/**
	 * 是否打印debug日志
	 * 
	 * @param logger
	 * @param content
	 */
	public static void printLog(org.slf4j.Logger logger, String content, boolean printDebugLog) {
		if (printDebugLog) {
			logger.info(content);
		}
	}

	/**
	 * 是否打印debug日志
	 * 
	 * @param logger
	 * @param content
	 */
	public static void printLog(org.slf4j.Logger logger, String format, ByteBuf content, boolean printDebugLog) {
		if (printDebugLog) {
			logger.info(format, ByteBufUtil.hexDump(content).toUpperCase());
		}
	}
	
	
	public static void printLog(org.apache.logging.log4j.Logger logger, String format, ByteBuf content, boolean printDebugLog) {
		if (printDebugLog) {
			logger.info(format, ByteBufUtil.hexDump(content).toUpperCase());
		}
	}
	

	/**
	 * 是否打印debug日志
	 * 
	 * @param logger
	 * @param content
	 */
	public static void printLog(org.slf4j.Logger logger, String format, boolean printDebugLog, Object... content) {
		if (printDebugLog) {
			logger.info(format, content);
		}
	}

}
