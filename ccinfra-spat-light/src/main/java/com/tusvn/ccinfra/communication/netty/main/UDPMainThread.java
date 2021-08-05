package com.tusvn.ccinfra.communication.netty.main;

/**
 * 海信平台信号灯----主启动类
 * 
 * @author Administrator
 *
 */
public class UDPMainThread {

	public static void main(String[] args) {
		CarCloudServer mainServer = CarCloudServer.getInstance();
		mainServer.start();
	}

}
