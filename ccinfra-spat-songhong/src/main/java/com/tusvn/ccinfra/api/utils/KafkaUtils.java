package com.tusvn.ccinfra.api.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.LoggerFactory;

import com.tusvn.ccinfra.config.ConfigProxy;

/**
 * @Auther: 赵云海
 * @Date: 2018/9/13 16:08
 * @Version: 1.0
 * @Description: TODO
 */
public class KafkaUtils {
    private static KafkaUtils intance;
    private Map<String, List<KafkaProducer<byte[], byte[]>>> producers;

    private KafkaUtils() {
        this.producers = new ConcurrentHashMap<>();
    }

    /**
     * 取得 KafkaUtisl 实例
     */
    public static KafkaUtils getInstance() {
        if (intance == null) {
            synchronized (KafkaUtils.class) {
                if (intance == null) {
                    intance = new KafkaUtils();
                }
            }
        }
        return intance;
    }

    /**
     * 创建新的KafkaProducer
     * @param bootstrap bootstrap
     * @param count count
     * @param props 附加选项
     */
    private synchronized void newProducer(String bootstrap, int count, Properties props) {
        if (producers.containsKey(bootstrap) && producers.get(bootstrap).size() >= count) {
            return;
        }
        if(props != null) {
            props.putIfAbsent("compression.type", "none");
            
            
            props.put("request.required.acks", "0");
            props.put("producer.type", "sync");
            
//            props.put("request.required.acks", "0");
//            props.put("producer.type", "async");
//            props.put("queue.buffering.max.messages", "1000");
//            props.put("queue.buffering.max.ms", "50");
//            props.put("batch.num.messages", "2");
            
            props.putIfAbsent("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.putIfAbsent("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        } else {
            props = getProducerProperties();
        }
        props.putIfAbsent("bootstrap.servers", bootstrap);
        producers.computeIfAbsent(bootstrap, x -> new ArrayList<>()).add(new KafkaProducer<>(props));
        logger.info("Create a new producer connection ->" + bootstrap);
    }

    /**
     * 返回 Produser 通用配置参数
     * @return
     */
    private Properties getProducerProperties(){
        Properties props = new Properties();
        
        
        props.put("request.required.acks", "0");
        props.put("producer.type", "sync");
//        props.put("queue.buffering.max.messages", "1000");
//        props.put("queue.buffering.max.ms", "50");
//        props.put("batch.num.messages", "2");
//        
        
        
        
        props.put("compression.type", "none");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return props;
    }

    /**
     * 返回一个消费者对象
     */
    public Consumer<byte[], byte[]> getConsumer(String bootstrap, String topic, String groupId) {
        return getConsumer(bootstrap, topic, groupId, OffsetReset.LATEST);
    }

    /**
     * 返回一个消费者对象
     */
    public Consumer<byte[], byte[]> getConsumer(String bootstrap, String topic, String groupId, OffsetReset offset) {
        return getConsumer(bootstrap, topic, groupId, offset, getComsumerProperties());
    }

    /**
     *
     * 返回一个消费者对象
     * @param bootstrap
     * @param topic
     * @param groupId
     * @param offset
     * @param consumerProperties
     * @return
     */
    public Consumer<byte[], byte[]> getConsumer(String bootstrap, String topic, String groupId, OffsetReset offset, Properties consumerProperties) {
        if (null == topic || "".equals(topic) || null == groupId || "".equals(groupId)) {
            return null;
        }

        if(consumerProperties == null || consumerProperties.size() == 0){
            consumerProperties = getComsumerProperties();
        }
        Properties props = consumerProperties;
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", offset.getLabel());
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public Consumer<String, String> getStringConsumer(String bootstrap, String topic, String groupId, OffsetReset offset, Properties consumerProperties) {
    	if (null == topic || "".equals(topic) || null == groupId || "".equals(groupId)) {
    		return null;
    	}
    	
    	if(consumerProperties == null || consumerProperties.size() == 0){
    		consumerProperties = getComsumerProperties();
    	}
    	Properties props = consumerProperties;
    	props.put("bootstrap.servers", bootstrap);
    	props.put("group.id", groupId);
    	props.put("auto.offset.reset", offset.getLabel());
    	Consumer<String, String> consumer = new KafkaConsumer<>(props);
    	consumer.subscribe(Arrays.asList(topic));
    	return consumer;
    }

    /**
     * 返回一个消费者对象
     */
    public Consumer<byte[], byte[]> getConsumerByRegex(String bootstrap, String topic, String groupId, OffsetReset offset) {
        return getConsumerByRegex(bootstrap, topic, groupId, offset, getComsumerProperties());
    }
    /**
     * 返回一个消费者对象
     */
    public Consumer<byte[], byte[]> getConsumerByRegex(String bootstrap, String topic, String groupId, OffsetReset offset, Properties consumerProperties) {
        if (null == topic || "".equals(topic) || null == groupId || "".equals(groupId)) {
            return null;
        }
        if(consumerProperties == null || consumerProperties.size() == 0){
            consumerProperties = getComsumerProperties();
        }
        Properties props = consumerProperties;
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", offset.getLabel());
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Pattern.compile("^" + topic + ".*"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // TODO Auto-generated method stub

            }
        });
        return consumer;
    }

    /**
     * 返回 KafkaConsumer 通用配置选项
     *
     * @return KafkaConsumer 通用配置选项
     */
    public Properties getComsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }
    public Properties getStringComsumerProperties() {
    	Properties props = new Properties();
    	props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    	props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    	props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    	return props;
    }
    /**
     * 获取producer
     **/
    public KafkaProducer<byte[], byte[]> getProducer() {
        return getProducer(getProducerProperties());
    }
    /**
     * 获取producer根据取模key
     **/
    public KafkaProducer<byte[], byte[]> getProducerByKey(int hash) {
        return getProducerByKey(getProducerProperties(),hash);
    }

    /**
     * 创建新的KafkaProducer
     * @param props 自定义扩展参数
     */
    public KafkaProducer<byte[], byte[]> getProducer(Properties props) {
        String bootstrap = ConfigProxy.getProperty("kafka.bootstrap");
        String count = ConfigProxy.getProperty("kafka.connection.count");
        return getProducer(bootstrap, Integer.parseInt(count), props);
    }
    /**
     * 创建新的KafkaProducer根据取模key
     * @param props 自定义扩展参数
     */
    public KafkaProducer<byte[], byte[]> getProducerByKey(Properties props,int hash) {
        String bootstrap = ConfigProxy.getProperty("kafka.bootstrap");
        String count = ConfigProxy.getProperty("kafka.connection.count");
        return getProducerByKey(bootstrap, Integer.parseInt(count), props,hash);
    }

    /**
     * 创建新的KafkaProducer
     * @param bootstrap bootstrap
     * @param count count
     */
    public KafkaProducer<byte[], byte[]> getProducer(String bootstrap, int count) {
        return getProducer(bootstrap, count, getProducerProperties());
    }

    /**
     * 创建新的KafkaProducer
     * @param bootstrap bootstrap
     * @param count count
     * @param props 附加选项
     */
    public KafkaProducer<byte[], byte[]> getProducer(String bootstrap, int count, Properties props) {
        if (!producers.containsKey(bootstrap)) {
            for (int i = 0; i < count; i++) {
                this.newProducer(bootstrap, count, props);
            }
        } else if (producers.containsKey(bootstrap) && producers.get(bootstrap).size() < count) {
            for (int i = 0; i < count - producers.get(bootstrap).size(); i++) {
                this.newProducer(bootstrap, count, props);
            }
        }
        return producers.get(bootstrap).get(new Random().nextInt(producers.get(bootstrap).size()));
    }
    /**
     * 创建新的KafkaProducer
     * @param bootstrap bootstrap
     * @param count count
     * @param props 附加选项
     */
    public KafkaProducer<byte[], byte[]> getProducerByKey(String bootstrap, int count, Properties props,int hash) {
        if (!producers.containsKey(bootstrap)) {
            for (int i = 0; i < count; i++) {
                this.newProducer(bootstrap, count, props);
            }
        } else if (producers.containsKey(bootstrap) && producers.get(bootstrap).size() < count) {
            for (int i = 0; i < count - producers.get(bootstrap).size(); i++) {
                this.newProducer(bootstrap, count, props);
            }
        }
        return producers.get(bootstrap).get(hash & (producers.get(bootstrap).size()-1));
    }

    /**
     * 发送数据信息
     **/
    public void send(String topic, String value, Callback callback) {
        getProducer().send(new ProducerRecord<>(topic, null, value.getBytes()), callback);
    }

    /**
     * 发送数据信息
     **/
    public void send(String topic, String value) {
    	getProducer().send(new ProducerRecord<>(topic, null, value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(String topic, String key, String value) {
        ////getProducer().send(new ProducerRecord<>(topic, key.getBytes(), value.getBytes()));
        
        KafkaProducer<byte[], byte[]> prod=getProducer();
        
        prod.send(new ProducerRecord<>(topic, key.getBytes(), value.getBytes()));
        
        prod.flush();
    }

    /**
     * 发送数据信息
     **/
    public void send(String topic, int hash, byte[] key, byte[] value) {
        int partition = hash % partitionsFor(getProducer(), topic).size();
        getProducer().send(new ProducerRecord<>(topic, partition, key, value));
    }

    /**
     * 发送数据信息
     **/
    public void send(TopicPartition tp, byte[] key, byte[] value) {
        getProducer().send(new ProducerRecord<>(tp.topic(), tp.partition(), key, value));
    }

    /**
     * 发送数据信息
     **/
    public void send(String topic, int hash, byte[] value) {
        int partition = hash % partitionsFor(getProducer(), topic).size();
        getProducer().send(new ProducerRecord<>(topic, partition, null, value));
    }

    /**
     * 发送数据信息
     **/
    public void send(TopicPartition tp, byte[] value) {
        getProducer().send(new ProducerRecord<>(tp.topic(), tp.partition(), null, value));
    }

    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, String topic, int hash, byte[] key, byte[] value) {
        int partition = hash % partitionsFor(producer, topic).size();
        producer.send(new ProducerRecord<>(topic, partition, key, value));
    }

    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, TopicPartition tp, byte[] key, byte[] value) {
        producer.send(new ProducerRecord<>(tp.topic(), tp.partition(), key, value));
    }

    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, String topic, int hash, byte[] value) {
        int partition = hash % partitionsFor(producer, topic).size();
        producer.send(new ProducerRecord<>(topic, partition, null, value));
    }

    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, TopicPartition tp, byte[] value) {
        producer.send(new ProducerRecord<>(tp.topic(), tp.partition(), null, value));
    }

    /**
     * 发送数据信息
     **/
    public void send(String topic, int hash, String key, String value) {
        int partition = hash % partitionsFor(getProducer(), topic).size();
        getProducer().send(new ProducerRecord<>(topic, partition, key.getBytes(), value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(TopicPartition tp, String key, String value) {
        getProducer().send(new ProducerRecord<>(tp.topic(), tp.partition(), key.getBytes(), value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(String topic, int hash, String value) {
        int partition = hash % partitionsFor(getProducer(), topic).size();
        getProducer().send(new ProducerRecord<>(topic, partition, null, value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(String topic, int hash, String value, Callback callback) {
        int partition = hash % partitionsFor(getProducer(), topic).size();
        getProducer().send(new ProducerRecord<>(topic, partition, null, value.getBytes()), callback);
    }
    /**
     * 发送数据信息
     **/
    public void send(String topic, int hash, String value, Callback callback,String key) {
        int partition = hash % partitionsFor(getProducer(), topic).size();
        getProducer().send(new ProducerRecord<>(topic, partition, key.getBytes(), value.getBytes()), callback);
    }
    /**
     * 根据取模key发送数据信息
     **/
    public void sendByKey(String topic, int hash, String value, Callback callback,String key) {
        int partition = hash % partitionsFor(getProducer(), topic).size();
        getProducerByKey(hash).send(new ProducerRecord<>(topic, partition, key.getBytes(), value.getBytes()), callback);
    }

    /**
     * 有序的发送数据
     * 1. 处理key值为null
     * 2. 通过hash获取Producer
     **/
    public void sendByKey(String topic, int hash, String key, String value) {
        int partition   = hash % partitionsFor(getProducer(), topic).size();
        byte[] bytesKey = (key == null)?null:key.getBytes();
        getProducerByKey(hash).send(new ProducerRecord<>(topic, partition, bytesKey, value.getBytes()));
    }


    /**
     * 发送数据信息
     **/
    public void send(TopicPartition tp, String value) {
        getProducer().send(new ProducerRecord<>(tp.topic(), tp.partition(), null, value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, String topic, int hash, String key, String value) {
        int partition = hash % partitionsFor(producer, topic).size();
        producer.send(new ProducerRecord<>(topic, partition, key.getBytes(), value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, TopicPartition tp, String key, String value) {
        producer.send(new ProducerRecord<>(tp.topic(), tp.partition(), key.getBytes(), value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, String topic, int hash, String value) {
        int partition = hash % partitionsFor(producer, topic).size();
        producer.send(new ProducerRecord<>(topic, partition, null, value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, TopicPartition tp, String value) {
        producer.send(new ProducerRecord<>(tp.topic(), tp.partition(), null, value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, String topic, String value) {
        producer.send(new ProducerRecord<>(topic, null, value.getBytes()));
    }


    /**
     * 发送数据信息
     **/
    public void send(KafkaProducer<byte[], byte[]> producer, String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key.getBytes(), value.getBytes()));
    }

    /**
     * 发送数据信息
     **/
    public void send(String topic, byte[] value) {
        getProducer().send(new ProducerRecord<>(topic, null, value));
    }

    /**
     * 发送数据信息
     *
     * @param topic
     * @param key
     * @param value
     */
    public void send(String topic, byte[] key, byte[] value) {
        getProducer().send(new ProducerRecord<>(topic, key, value));
    }

    /**
     * 发送数据信息
     *
     * @param producer
     * @param topic
     * @param value
     */
    public void send(KafkaProducer<byte[], byte[]> producer, String topic, byte[] value) {
        producer.send(new ProducerRecord<>(topic, null, value));
    }

    /**
     * 关闭连接
     *
     * @param producer
     * @param topic
     * @param key
     * @param value
     */
    public void send(KafkaProducer<byte[], byte[]> producer, String topic, byte[] key, byte[] value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    /**
     * 获取分区信息
     *
     * @param topic
     * @return
     */
    public List<PartitionInfo> partitionsFor(KafkaProducer<byte[], byte[]> producer, String topic) {
        return producer.partitionsFor(topic);
    }

    /**
     * 关闭连接
     */
    public void close() {
        String bootstrap = ConfigProxy.getProperty("kafka.bootstrap");
        if (bootstrap == null) {
            logger.error("Please configure bootstrap in Apollo.");
        }
        this.close(bootstrap);
    }

    /**
     * 关闭连接
     *
     * @param bootstrap
     */
    public void close(String bootstrap) {
        List<KafkaProducer<byte[], byte[]>> producers = this.producers.remove(bootstrap);
        if (producers != null) {
            producers.forEach(x -> x.close());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        //String bootstrap = "172.17.1.9:9092,172.17.1.10:9092,172.17.1.11:9092";
        String bootstrap = ConfigProxy.getProperty("kafka.bootstrap");
        System.out.println(bootstrap);
        //KafkaProducer<byte[], byte[]> producer = KafkaUtils.getInstance().getProducer(bootstrap, 1);
        Consumer<byte[], byte[]> consumer = KafkaUtils.getInstance().getConsumer(bootstrap, "Rc_VEH_Data_Basic", UUID.randomUUID().toString());
        /*while (true) {
            //String data = "{\"v2xDataType\":\"V2X_BSM\",\"message\":[{\"msgCnt\":0,\"vehicleId\":\"B21E-00-00" + (new Random().nextInt(5) + 1) + "\",\"gpstime\":1563789683107,\"longitude\":121.14128008179549,\"latitude\":31.2552229471111,\"altitude\":0.0,\"speed\":14.951354536633007,\"routeId\":\"right\",\"roadId\":\"101\",\"laneid\":\"0\",\"lanePos\":256.0471812985572}]}";
            //KafkaUtils.getInstance().send(producer, "Rc_RSU_Data", new Random().nextInt(8), data.getBytes());
            //Thread.sleep(20);
            for (ConsumerRecord<byte[],byte[]> record: consumer.poll(1)) {
                System.out.println(new String(record.value()));
                //String[] s = new String(record.value()).split("\\|");
                //System.out.println(System.currentTimeMillis() - Long.parseLong(s[0]));
            }
        }*/
    }

    /**
     * 日志定义 Logger
     */
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaUtils.class);
}
