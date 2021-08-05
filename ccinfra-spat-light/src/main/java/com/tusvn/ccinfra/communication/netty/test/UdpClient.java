package com.tusvn.ccinfra.communication.netty.test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.alibaba.druid.util.HexBin;
import com.tusvn.ccinfra.communication.netty.util.ParseUtils;
import com.tusvn.ccinfra.communication.netty.util.SpatUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.CharsetUtil;

public class UdpClient {
	public static final int MessageReceived = 0x99;

	private static class CLientHandler extends SimpleChannelInboundHandler<DatagramPacket> {

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
			String body = packet.content().toString(CharsetUtil.UTF_8);
//			System.out.println(body);
		}
	}

	public static void main(String[] args) {

		EventLoopGroup group = new NioEventLoopGroup();
		try {
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioDatagramChannel.class).handler(new CLientHandler());

			String filepath = "C:\\Users\\tsing\\Desktop\\7.txt";
			
			if(args.length > 0) {
				filepath = args[0];
			}
			
			////List<String> readLines = FileUtils.readLines(new File(filepath), "utf-8");
			Channel ch = b.bind(0).sync().channel();
			for (int i = 0; i < 10; i++) {
			//for (String line : readLines) {
				/////byte[] decode = HexBin.decode(line);
				////byte[] decode = SpatUtils.getSendBytes();
//					System.out.println(decode.length);
				
				
				ByteBuf out2=SpatUtils.buildTestSend();
				
				 byte[] dst=new byte[out2.readableBytes()];		
				 out2.readBytes(dst);
				 
				 ParseUtils.printByteArray("dst", dst);
				
				
				
//				ByteBuf out = ch.alloc().directBuffer(1000);
//				//开始
//				out.writeByte(0xc0);
//				// 消息标识
//				out.writeShort(0);
//				// 发送id
//				byte[] sendId = new byte[] {7,0,0,0,0,0};
//				out.writeBytes(sendId);
//				// 接收id
//				byte[] receiveId = new byte[6];
//				out.writeBytes(receiveId);
//				// TUC 时间
//				out.writeInt((int) (System.currentTimeMillis()/1000));
//				// 毫秒数
//				out.writeShort((int) (System.currentTimeMillis()%1000));
//				// 生命周期
//				out.writeByte(0x10);
//				// 版本号（0x10）
//				out.writeByte(0x10);
//				// 操作类型
//				out.writeByte(0x87);
//				// 操作对象
//				out.writeByte(0x01);
//				// 200 字节预留
//				byte[] reserved = new byte[200];
//				out.writeBytes(reserved);
//				// 消息长度
//				out.writeShortLE(224 + decode.length + 10 );
//				// 经度
//				out.writeFloatLE(109.55625F);
//				// 纬度
//				out.writeFloatLE(24.418776F);
//				// 路口范围
//				out.writeByte(1);
//				// 进口方向数
//				out.writeByte(4);
//				
//				//ByteBuf directBuffer = ch.alloc().directBuffer(1000);
//				out.writeBytes(decode);
				ch.writeAndFlush(new DatagramPacket(Unpooled.copiedBuffer(out2), new InetSocketAddress("255.255.255.255", 50002))).sync(); ///
				
				////System.out.println("发送数据："+ByteBufUtil.hexDump(out2).toUpperCase());
				
				////System.out.println("发送数据："+Arrays.toString(decode));
			    System.out.println("发送数据："+i);
				Thread.sleep(1000);
			//}
			}
			ch.closeFuture().await();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			group.shutdownGracefully();
		}
	}

	public static byte[] hexToByteArray(String inHex) {
		int hexlen = inHex.length();
		byte[] result;
		if (hexlen % 2 == 1) {
			// 奇数
			hexlen++;
			result = new byte[(hexlen / 2)];
			inHex = "0" + inHex;
		} else {
			// 偶数
			result = new byte[(hexlen / 2)];
		}
		int j = 0;
		for (int i = 0; i < hexlen; i += 2) {
			result[j] = hexToByte(inHex.substring(i, i + 2));
			j++;
		}
		return result;
	}

	public static byte hexToByte(String inHex) {
		return (byte) Integer.parseInt(inHex, 16);
	}
}
