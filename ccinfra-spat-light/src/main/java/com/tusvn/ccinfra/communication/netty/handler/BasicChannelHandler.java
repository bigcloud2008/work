/**
 *
 */
package com.tusvn.ccinfra.communication.netty.handler;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
//import com.tusvn.ccinfra.api.utils.AlgorithmUtils;
import com.tusvn.ccinfra.config.ConfigProxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 *
 * @author xchao on 2019/03/27.
 */
public abstract class BasicChannelHandler<T> extends SimpleChannelInboundHandler<T> {
	/**
	 * 日志 sfl4j 注册
	 */
	protected org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicChannelHandler.class);

	protected static List<KafkaProducer<String, String>> producers = Lists.newArrayList();

	/////////////-----------设备id和路口id转换关系
	protected static Map<String, String> roadIdMap = initRoadIdMapNew();

	protected int port;

	public BasicChannelHandler(int port) {
		this.port = port;
	}

	/**
	 * 初始化kakfa连接
	 * 
	 * @return
	 */
	public static void initProducers() {
		// 创建kafkaProperties
		Properties properties = getKafkaProperties();
		for (int i = 0; i < ConfigProxy.getInt("kafka.connection.count"); i++) {
			producers.add(new KafkaProducer<String, String>(properties));
		}
	}

	/**
	 * 创建kafkaProperties
	 * 
	 * @return
	 */
	private static Properties getKafkaProperties() {
		Properties props = new Properties();
		props.put("compression.type", "none");
		props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("bootstrap.servers", ConfigProxy.getProperty("kafka.bootstrap"));
		return props;
	}

	/**
	 * 初始化路口id和监听端口关系
	 * 
	 * @return
	 */
	private Map<Integer, String> initRoadIdMap() {
		Map<Integer, String> roadIdMap = Maps.newHashMap();
		for (String portCrossing : ConfigProxy.getProperty("Spat_Port_Crossing_Map").split(",")) {
			String[] split = portCrossing.split(":");
			roadIdMap.put(Integer.parseInt(split[1]), split[0]);
		}
		return roadIdMap;

	};
	
	/**********
	 * 发送者设备编码转换为路口id，百度RSCU这里直接配置为crossid
	 * 
	 * 
	 * 
	 * ***/
	private static Map<String, String> initRoadIdMapNew() {
		Map<String, String> roadIdMap = Maps.newHashMap();
		
		String spatDeviceCrossMap=ConfigProxy.getPropertyOrDefault("spat.deviceToCrossing", "1000:8,1001:9");
		
		for (String portCrossing : spatDeviceCrossMap.split(",")) {
			String[] split = portCrossing.split(":");
			roadIdMap.put(split[1], split[0]);
		}
		return roadIdMap;

	};
	

	protected String joinTopic(String topic) {
		return StringUtils.join(ConfigProxy.getProperty("topic.prefix").replaceAll("null", ""), topic);
	}

	/**
	 * 获取Producer
	 * 
	 * @param hash
	 * @return
	 */
	private KafkaProducer<String, String> getKafkaProducer(int hash) {
		return producers.get(hash % producers.size());
	}

	/**
	 * 发送数据到 Kafka
	 * 
	 * @param topic
	 * @param key
	 * @param value
	 */
	protected void sendDataToKafka(String topic, String key, String value) {
		int hash = key.hashCode();
		if(hash<0)
			hash=-1*hash;
		
		// 获取 producer
		KafkaProducer<String, String> producer = this.getKafkaProducer(hash);
		int partition = hash % producer.partitionsFor(topic).size();
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition, key, value);
		producer.send(record);
	}

	/**
	 * 消息读取完毕有执行
	 *
	 * @param ctx
	 */
	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	/**
	 * 消息读取完毕有执行
	 *
	 * @param ctx
	 * @param
	 * @throws Exception
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable ex) throws Exception {
		logger.error(ex.getMessage(), ex);
	}

}
