package com.tusvn.ccinfra.communication.netty.main;

import com.tusvn.ccinfra.communication.netty.NettyService;

/**
 * 车云网关主启动类
 * 
 * @author Administrator
 *
 */
public class CarCloudServer {

	private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CarCloudServer.class);
	private static CarCloudServer instance;

	/**
	 * 防止私有化
	 */
	private CarCloudServer() {
	}

	/**
	 * 返回 CarCloudServer 实例
	 * 
	 * @return CarCloudServer 实例
	 */
	public static CarCloudServer getInstance() {
		if (instance == null) {
			synchronized (CarCloudServer.class) {
				if (instance == null) {
					instance = new CarCloudServer();
				}
			}
		}
		return instance;
	}

	/**
	 * 启动服务
	 */
	public void start() {
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					// 主服务
					NettyService service = NettyService.getInstance();
					// 启动
					service.start();
				} catch (Exception e) {
					logger.error("CarCloudServer 启动过程发生错误！", e);
				}
			}
		}, "car-cloud-server-main-thread");
		t.start();
	}
}
