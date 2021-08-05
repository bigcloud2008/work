package com.tusvn.ccinfra.communication.netty;

import java.net.DatagramSocket;
import java.net.InetAddress;

import org.apache.commons.lang3.StringUtils;

//import com.tusvn.ccinfra.api.state.StateNode;
//import com.tusvn.ccinfra.api.utils.CuratorClientUtil;
import com.tusvn.ccinfra.communication.netty.handler.SpatHandler;
import com.tusvn.ccinfra.communication.netty.util.StringBuilderHolder;
import com.tusvn.ccinfra.config.ConfigProxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @author xujun on 2018/5/11.
 */
public class NettyService {
	/**
	 * 日志 sfl4j 注册
	 */
	private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NettyService.class);
	private EventLoopGroup bossGroup;

	private static NettyService instance;

	/**
	 * 返回 TcpServerConfigService 配置服务实例
	 * 
	 * @return TcpServerConfigService 实例
	 */
	public static NettyService getInstance() {
		if (instance == null) {
			synchronized (NettyService.class) {
				if (instance == null) {
					instance = new NettyService();
				}
			}
		}
		return instance;
	}

	private NettyService() {

	}

	private StringBuilder addParam(StringBuilder sb, String key, Object value) {
		if (StringUtils.isNotEmpty(key)) {
			sb.append(key).append(": ");
		}
		sb.append(value);
		sb.append("\n");
		return sb;
	}

	/**
	 * 启动主服务
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {
		int availableProcessors = Runtime.getRuntime().availableProcessors();
		int bossgroupSize = ConfigProxy.getIntOrDefault("bossGroupSize", 1);

		String osName = System.getProperty("os.name").toLowerCase();
		boolean isUnixOrLinux = osName.contains("linux") || osName.contains("unix");
		boolean epollisAvailable = io.netty.channel.epoll.Epoll.isAvailable();
		if (isUnixOrLinux && epollisAvailable) {
			// bossGroup线程池用来接受客户端的连接请求
			bossGroup = new EpollEventLoopGroup(bossgroupSize);
		} else {
			bossGroup = new NioEventLoopGroup(bossgroupSize);
		}

		try {
			
			///////////////////////////////
			String serviceName=ConfigProxy.getPropertyOrDefault("spat.serviceName", "spat");
			String run_path=System.getProperty("user.dir");
			int    pid     = com.tusvn.ccinfra.communication.netty.util.SpatUtils.getPid();
			String  w_res  =  com.tusvn.ccinfra.communication.netty.util.SpatUtils.writePid(run_path, serviceName, pid);
			logger.info("===============>serviceName="+serviceName+", pid="+pid);
			///////////////////////////////
			
			
			
			Bootstrap bootstrap = new Bootstrap();
			bootstrap.group(bossGroup);
			
			
			///////////tcp---通信
			
//    		if (epollisAvailable) {
//    			bootstrap.channel(EpollSocketChannel.class);
//    		} else {
//    			bootstrap.channel(NioSocketChannel.class);
//    		}
			
			/////////udp-----通信
			if (epollisAvailable) {
				bootstrap.channel(EpollDatagramChannel.class);
			} else {
				bootstrap.channel(NioDatagramChannel.class);
			}

			bootstrap.option(ChannelOption.SO_BROADCAST, true);
			bootstrap.option(ChannelOption.SO_RCVBUF, 1024 * 1024);

			
			
			///////---------Spat_Port_Crossing_Map=====>8:50002
			String[] Spat_Port_Crossing_Map = ConfigProxy.getProperty("Spat_Port_Crossing_Map").split(",");
			for (int i = 0; i < Spat_Port_Crossing_Map.length; i++) {
				String[] port_Crossing = Spat_Port_Crossing_Map[i].split(":");
				String crossingId = port_Crossing[0];
				int port = Integer.parseInt(port_Crossing[1]);
				// add parm to StringBuilder
				StringBuilder sb = new StringBuilderHolder(1024).get();
				addParam(sb, "", "");
				addParam(sb, null, StringUtils.leftPad("#", 120, "#"));
				addParam(sb, "os.name", osName);
				addParam(sb, "server.uid", getLocalIp());
				addParam(sb, "server.port", port);
				addParam(sb, "bossgroupSize", bossgroupSize);
				addParam(sb, "availableProcessors", availableProcessors);
				addParam(sb, "epollisAvailable", epollisAvailable);
				addParam(sb, "bossGroup.class", bossGroup.getClass().getCanonicalName());
				// add childOption to param
				addParam(sb, "childOption.SO_BROADCAST", true);
				addParam(sb, "childOption.SO_RCVBUF", 1024 * 1024);
				addParam(sb, "bootstart", bootstrap);
				addParam(sb, null, StringUtils.leftPad("#", 120, "#"));
				logger.info(sb.toString());
				
				System.out.println("==========>crossingId="+crossingId+", port="+port);

				bootstrap.handler(new SpatHandler(port));

				// Start the server.
				ChannelFuture future = bootstrap.bind(port).sync();
				future.channel().closeFuture().addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						// 通过回调只关闭自己监听的channel
						future.channel().close();
					}
				});
				logger.info("spat-NettyUdpService 启动成功！路口id:" + crossingId + "	port:" + port);   
				// TCP-SERVER注册到zookeeper
				////StateNode.registerServerNode(port);
				// Wait until the server socket is closed.
			}
			// 启动成功后立即创建 KafkaProducer，提前创建KafkaProducer（该过程会创建一批 Producer）
			SpatHandler.initProducers();
		} catch (Throwable ex) {
			logger.error("spat-NettyUdpService 启动出现异常！", ex);
		} finally {
		}
	}

	public void stop() {
		// 删除zookeeper中的TCP-SERVER
		////CuratorClientUtil.destory();
		bossGroup.shutdownGracefully();
	}

	/**
	 * 获取本机ip
	 *
	 * @return
	 */
	private static String getLocalIp() {
		try {
			@SuppressWarnings("resource")
			DatagramSocket socket = new DatagramSocket();
			socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
			return socket.getLocalAddress().getHostAddress();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}

}
