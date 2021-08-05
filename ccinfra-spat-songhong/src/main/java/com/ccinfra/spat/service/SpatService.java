package com.ccinfra.spat.service;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ccinfra.spat.domain.SpatData;
import com.ccinfra.spat.domain.SpatDataInfo;
import com.tusvn.ccinfra.api.utils.KafkaUtils;

import com.tusvn.ccinfra.config.ConfigProxy;
import com.tusvn.ccinfra.config.uitls.CfgUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


/*******
 * 上海松鸿信号灯数据---10号路口
 * 
 * 
 * 
 * ***/
public class SpatService implements Runnable{
	
	private static final Logger logger = LogManager.getLogger(SpatService.class);
 
	
	
	///////private static ExecutorService thread_pool = Executors.newFixedThreadPool(thread_num);
	
	private static ThreadPoolExecutor executorService=null;
	
	private KafkaConsumer<String, String> consumer=null;
 
	


	public SpatService() {
		// TODO Auto-generated constructor stub
		
        Properties properties = new Properties();
        properties.put("bootstrap.servers", 	CfgUtils.kafka_server);

        properties.put("key.deserializer", 	    "org.apache.kafka.common.serialization.StringDeserializer"); 
        properties.put("value.deserializer", 	"org.apache.kafka.common.serialization.StringDeserializer");

        properties.put("group.id", 			    CfgUtils.groupId);
        
        properties.put("auto.offset.reset",     "latest");
 
        
        logger.info("=========>kafka_server="+CfgUtils.kafka_server);
        logger.info("=========>topics="      +CfgUtils.topics);
        logger.info("=========>groupId="     +CfgUtils.groupId);
        
        logger.info("=========>thread_num="     +CfgUtils.thread_num);
 
		
		///2. 创建消费者
		this.consumer = new KafkaConsumer<>(properties);
		
		///3. 订阅主题
		consumer.subscribe(Arrays.asList(CfgUtils.topics));
		
		
		// 阻塞队列容量声明为100个
        executorService = new ThreadPoolExecutor(CfgUtils.thread_num, CfgUtils.thread_num, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(100));

        // 设置拒绝策略
        executorService.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 空闲队列存活时间
        executorService.setKeepAliveTime(20, TimeUnit.SECONDS);
        
	
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		 ////consumer.subscribe(Arrays.asList(topics));
		
		while(true){
			try {
				// 本例使用1000ms作为获取超时时间
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(CfgUtils.pull_timeout));
				
 
				
				/////////ArrayList<String> lines = new ArrayList<>();
				
				long cnt=0;
				
				for (ConsumerRecord<String, String> record : records) {
					
					
					if(cnt>=Long.MAX_VALUE)
						cnt=0;
					
					cnt++;
					
					System.out.println("========================>cnt="+cnt);
					
					String value = record.value();//////----------spat的json
					
					boolean  pringLog      =ConfigProxy.getBooleanOrDefault("spat.printLog", true);
					
					if(pringLog)
					    logger.info("===>src_spat="+value);
					
					
					executorService.execute(()->{
					
						/////////KafkaUtils.getInstance().send(send_topics, tmp_key, value);
						
						try {
							
							long x0=System.currentTimeMillis();
							SpatDataInfo spDataInfo=SpatConvertService.convertSpatData(value);
							long x1=System.currentTimeMillis();
							logger.info("x1-x0,usetime="+(x1-x0)+"ms" );
							
							SpatData spData=spDataInfo.getSpData();
							
							
							logger.info("spData="+JSON.toJSONString(spData, SerializerFeature.DisableCircularReferenceDetect) );
							
							
							////////////1.写redis
							SpatConvertService.writeRedis(spData); //////里面时间单位为秒
							
							
							long x2=System.currentTimeMillis();
							logger.info("x2-x1,usetime="+(x2-x1)+"ms" );
							
							
							/////////////2.写kafka
							Integer roadId  =spDataInfo.getCrossId();
							Integer regionId=spDataInfo.getRegionId();
							
							
							SpatData converted_spData=SpatConvertService.transferTime(spData);/////////时间单位转换为0.1s
						
							SpatConvertService.writeKafka(roadId, regionId, converted_spData);
							
							
							long x3=System.currentTimeMillis();
							logger.info("x3-x2,usetime="+(x3-x2)+"ms" );
							
 
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							logger.error("in send kafka", e);
						}
					
					});
 
 
				
				}
				
			    Thread.sleep(5L);  /////毫秒
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				logger.error("in run:"+ex);
				
				
			}
		}
		
		
		
	}

}