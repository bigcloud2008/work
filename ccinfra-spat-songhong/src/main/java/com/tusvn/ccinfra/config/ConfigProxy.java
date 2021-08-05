package com.tusvn.ccinfra.config;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.openapi.client.ApolloOpenApiClient;
import com.ctrip.framework.apollo.openapi.dto.OpenNamespaceDTO;
import com.google.common.collect.Maps;
import com.tusvn.ccinfra.config.backtype.KeyValue;
import com.tusvn.ccinfra.config.uitls.UtilsCommons;
import org.apache.commons.lang3.StringUtils;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.encryption.pbe.config.EnvironmentStringPBEConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @Auther: 赵云海
 * @Date: 2019/4/23 13:36
 * @Version: 1.0
 * @Description: 配置中心程序代理
 */
public class ConfigProxy {
    private static String algorithm = "PBEWithMD5AndDES";
    private static final String PREFIX = "ENC(";
    private static final String SUFFIX = ")";
    private static String password = "password";
    private static final Logger logger = LoggerFactory.getLogger(ConfigProxy.class);
    private static String namespace;
    private static Map<String, Properties> props;
    private static Map<String, Map<String, String>> namespaces;
    private static Map<KeyValue, Set<KeyValue>> relationNodes;
    private static StandardPBEStringEncryptor encryptor;
    private static Lock lock = new ReentrantLock();
    private static Map<String, KeyChangedListener> changedListenerMap;

    static {
        try {
			relationNodes = new ConcurrentHashMap<>();
			namespaces = new ConcurrentHashMap<>();
			props = new ConcurrentHashMap<>();
			changedListenerMap = Maps.newConcurrentMap();
			initEnvironment();
			init(namespace);
			encryptor();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

    }


    /**
     * 获取并初始化环境信息
     *
     * @return
     */
    private static void initEnvironment() {
        String apollo_web_meta = System.getProperty("apollo.web.meta");
        String apollo_cluster = System.getProperty("apollo.cluster");
        String apollo_meta = System.getProperty("apollo.meta");
        String appID = System.getProperty("app.id");
        String environment = System.getProperty("env");
        namespace = System.getProperty("namespace");
        if (StringUtils.isBlank(appID)) {
            System.setProperty("app.id", getLocalProperty("config.properties", "app.id"));
        }
        if (StringUtils.isBlank(namespace)) {
            namespace = getLocalProperty("config.properties", "apollo.default.namespace");
        }
        if (StringUtils.isBlank(apollo_meta)) {
            System.setProperty("apollo.meta", getLocalProperty("config.properties", "apollo.meta"));
        }
        if (StringUtils.isBlank(apollo_web_meta)) {
            System.setProperty("apollo.web.meta", getLocalProperty("config.properties", "apollo.web.meta"));
        }
        if (StringUtils.isBlank(environment)) {
            System.setProperty("env", getLocalProperty("config.properties", "apollo.env"));
        }
        if (StringUtils.isBlank(apollo_cluster)) {
            System.setProperty("apollo.cluster", getLocalProperty("config.properties", "apollo.cluster"));
        }
    }

    /**
     * 设置默认namespace, 将程序的默认配置实例更改为指定的实例
     * 相当于运行中修改dev.properties 中的 apollo.default.namespace 字段
     * 更改后会影响其他配置，谨慎使用
     */
    @SuppressWarnings("unchecked")
    public static void namespace(String defaultNamespace) {
        namespace = defaultNamespace;
    }

    /**
     * 初始化
     */
    synchronized static void init(String namespace) {
        if (namespaces.containsKey(namespace) || !getNamespaces().contains(namespace)) return;
        addChangeListener(namespace);
    }

    /**
     * 为指定配置空间添加监听,此方法为线程安全方法, 但不阻塞
     *
     * @param namespace
     */
    public static void addChangeListener(String namespace) {
        if (namespaces.putIfAbsent(namespace, new HashMap()) != null) return;
        ConfigService.getConfig(namespace).addChangeListener((e) -> {
            e.changedKeys().forEach(key -> {
                removeParentNode(e.getNamespace(), key);
                refresh(e.getNamespace(), key);
                cache(e.getNamespace(), key, e.getChange(key).getNewValue());
                onKeyChanged(namespace, key, e.getChange(key));
            });
        });
        Config config = ConfigService.getConfig(namespace);
        config.getPropertyNames().forEach(x -> cache(namespace, x, config.getProperty(x, null)));
    }

    /**
     * 当某个值变化的时候，刷新所有引用该值得变量
     *
     * @param namespace
     * @param key
     */
    private static void refresh(String namespace, String key) {
        for (KeyValue keyValue : getParentNode(namespace, key)) {
            String value = ConfigService.getConfig(keyValue.getKey()).getProperty(keyValue.getValue(), null);
            cache(keyValue.getKey(), keyValue.getValue(), value);
            refresh(keyValue.getKey(), keyValue.getValue());
        }
    }


    /**
     * 设置新的配置信息
     *
     * @param namespace
     * @param key
     * @param value
     */
    private static void cache(String namespace, String key, String value) {
        if (null == value) {
            namespaces.get(namespace).remove(key);
        } else {
            value = getQuoteIteraValue(namespace, key, value);
            namespaces.get(namespace).put(key, value);
        }
        logger.info("[apollo] " + namespace + ", " + (null == value ? "remove" : "add") + " -> " + key + "=" + value);
    }

    /**
     * 添加关联key信息
     *
     * @param key
     * @param parent
     */
    public static void addParentNode(KeyValue key, KeyValue parent) {
        if (parent != null) {
            relationNodes.computeIfAbsent(key, x -> new HashSet()).add(parent);
        }
    }

    /**
     * 获取关联key信息
     *
     * @param key
     */
    private static Set<KeyValue> getParentNode(String namespace, String key) {
        return relationNodes.getOrDefault(new KeyValue(namespace, key), new HashSet<>());
    }

    /**
     * 获取关联key信息
     *
     * @param key
     */
    private static void removeParentNode(String namespace, String key) {
        for (Map.Entry<KeyValue, Set<KeyValue>> entry : relationNodes.entrySet()) {
            entry.getValue().remove(new KeyValue(namespace, key));
        }
    }

    /**
     * 获取引用的真实值
     *
     * @param namespace
     * @param value
     * @return
     */
    public static String getQuoteIteraValue(String namespace, String key, String value) {
        try {
            return UtilsCommons.find(namespace, key, value);
        } catch (Exception e) {
            return value;
        }
    }


    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return getProperty(namespace, key);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static String getPropertyOrDefault(String key, String defaultValue) {
        return getPropertyOrDefault(namespace, key, defaultValue);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static int getInt(String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : Integer.parseInt(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static boolean getBoolean(String key) {
        String value = getProperty(namespace, key);
        return value == null ? false : Boolean.parseBoolean(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static boolean getBooleanOrDefault(String key, boolean defaultValue) {
    	//String value = getProperty(namespace, key);
    	String value = getPropertyOrDefault(namespace, key, String.valueOf(defaultValue));
    	return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static int getInt(String namespace, String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : Integer.parseInt(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static long getLong(String namespace, String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : Long.parseLong(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static long getLong(String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : Long.parseLong(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static double getDouble(String namespace, String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : Double.parseDouble(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static double getDouble(String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : Double.parseDouble(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static List<String> getList(String namespace, String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : Arrays.asList(value.split(","));
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static List<String> getList(String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : Arrays.asList(value.split(","));
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static JSONObject getJson(String namespace, String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : JSONObject.parseObject(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static JSONObject getJsonOrDefault(String namespace, String key, JSONObject def) {
        String value = getPropertyOrDefault(namespace, key, null);
        return value == null ? def : JSONObject.parseObject(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static JSONArray getJsonArray(String namespace, String key) {
        String value = getProperty(namespace, key);
        return value == null ? null : JSONArray.parseArray(value);
    }
    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static int getIntOrDefault(String key, int defaultValue) {
        String value = getPropertyOrDefault(namespace, key, String.valueOf(defaultValue));
        return value == null ? null : Integer.parseInt(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static String getPropertyOrDefault(String namespace, String key, String defaultValue) {
        if (!namespaces.containsKey(namespace)) {
            init(namespace);
        }
        String value = namespaces.getOrDefault(namespace, new HashMap<>()).get(key);
        if (value == null) {
            value = getEnvProperty(key);
        }
        if (value == null) {
            value = defaultValue;
        }
        /*if (value == null) {
            logger.error("Please configure " + key + " in Apollo <Namespace : " + namespace + ">.");
            throw new RuntimeException("Please configure " + key + " in Apollo <Namespace : " + namespace + ">.");
        }*/
        return decrypt(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static boolean containsKey(String namespace, String key) {
        if (!namespaces.containsKey(namespace)) {
            init(namespace);
        }
        boolean b = namespaces.getOrDefault(namespace, new HashMap<>()).containsKey(key);
        if (!b) {
            b = containsKeyInDefault(key);
        }
        return b;
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static String getProperty(String namespace, String key) {
        if (!namespaces.containsKey(namespace)) {
            init(namespace);
        }
        String value = namespaces.getOrDefault(namespace, new HashMap<>()).get(key);
        if (value == null) {
            value = getEnvProperty(key);
        }
        if (value == null) {
            logger.error("Please configure " + key + " in Apollo <Namespace : " + namespace + ">.");
            throw new RuntimeException("Please configure " + key + " in Apollo <Namespace : " + namespace + ">.");
        }
        return decrypt(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static String getEnvProperty(String key) {
        if (!namespaces.containsKey(getEnvNamespace())) {
            init(getEnvNamespace());
        }
        String value = namespaces.getOrDefault(getEnvNamespace(), new HashMap<>()).get(key);
        return decrypt(value);
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static boolean containsKeyInDefault(String key) {
        if (!namespaces.containsKey(getEnvNamespace())) {
            init(getEnvNamespace());
        }
        return namespaces.getOrDefault(getEnvNamespace(), new HashMap<>()).containsKey(key);
    }

    /**
     * 通过key前缀获取配置信息
     *
     * @param keyPre
     * @return
     */
    public static Map<String, String> getPropertyByPre(String keyPre) {
        return getPropertyByPre(namespace,keyPre);

    }
    /**
     * 通过key前缀获取配置信息
     *
     * @param keyPre
     * @return
     */
    public static Map<String, String> getPropertyByPre(String namespace,String keyPre) {
        if (!namespaces.containsKey(namespace)) {
            init(namespace);
        }
        Map<String, String> map = new HashMap<>();
        Set<String> keySet = namespaces.getOrDefault(namespace, new HashMap<>()).keySet();
        keySet.forEach(key -> {
            if (key.indexOf(keyPre) > -1) {
                String value = namespaces.getOrDefault(namespace, new HashMap<>()).get(key);
                if (value == null) {
                    logger.error("Please configure " + key + " in Apollo <Namespace : " + namespace + ">.");
                    throw new RuntimeException("Please configure " + key + " in Apollo <Namespace : " + namespace + ">.");
                }
                map.put(key, decrypt(value));
            }
        });

        return map;
    }

    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static boolean containsKey(String key) {
        if (!namespaces.containsKey(namespace)) {
            init(namespace);
        }
        return namespaces.containsKey(key);
    }

    public static boolean contansKeyNew(String key){
        if (!namespaces.containsKey(namespace)) {
            init(namespace);
        }
        return namespaces.getOrDefault(namespace, new HashMap<>()).containsKey(key);
    }
    /**
     * 获取配置信息
     */
    public static String getLocalProperty(String file, String key) {
        if (!props.containsKey(file)) {
            Properties properties = UtilsCommons.getProperties(file);
            props.putIfAbsent(file, properties);
            properties.stringPropertyNames().forEach(x -> logger.info("[local] " + file + ", add -> " + x + "=" + properties.getProperty(x)));
        }
        return props.getOrDefault(file, new Properties()).getProperty(key);
    }

    /**
     * 获取配置信息
     */
    public static String getEnvNamespace() {
        return getLocalProperty("config.properties", "apollo.env.namespace");
    }

    /**
     * 获取所有的OpenNamespaceDTO
     *
     * @return
     */
    public static List<OpenNamespaceDTO> getOpenNamespaceDTO() {
        ApolloOpenApiClient build = ApolloOpenApiClient.newBuilder().withPortalUrl(System.getProperty("apollo.web.meta"))
                .withToken(getLocalProperty("config.properties", "apollo.token")).build();
        return build.getNamespaces(System.getProperty("app.id"), System.getProperty("env"), System.getProperty("apollo.cluster"));
    }

    /**
     * 获取所有的OpenNamespaceDTO
     *
     * @return
     */
    public static List<String> getNamespaces() {
        return getOpenNamespaceDTO().stream().map(x -> x.getNamespaceName()).collect(Collectors.toList());
    }

    /**
     * 对加密配置进行解密
     */

    static void encryptor() {
        // 加密工具
        encryptor = new StandardPBEStringEncryptor();
        // 加密配置
        EnvironmentStringPBEConfig config = new EnvironmentStringPBEConfig();
        config.setAlgorithm(algorithm);
        // 自己在用的时候更改此密码
        config.setPassword(password);
        // 应用配置
        encryptor.setConfig(config);
    }

    /**
     * 对加密配置进行解密
     */
    static String decrypt(String property) {
        if (StringUtils.isNotBlank(property)) {
            if (property.startsWith(PREFIX)) {
                property = encryptor.decrypt(property.substring(3, property.length()));
            }
        }
        return property;
    }


    /**
     * 动态主题类型添加前缀
     *
     * @param t
     * @return
     */
    public static String prefix(String t) {
        String prefix = getPropertyOrDefault("topic.prefix", "");
        if (t == null)
            throw new NullPointerException();
        else
            return prefix + t;
    }

    // 监听Key的变化并处理
    // 添加Key变更监听, 记得删除它
    public static void addKeyChangedListener(String namespace, String key, KeyChangedListener listener){
        String k = namespace + "." + key;
        changedListenerMap.put(k,listener);
    }

    // 删除Key变更监听
    public static void delKeyChangedListener(String namespace, String key){
        String k = namespace + "." + key;
        if(changedListenerMap.containsKey(k)){
            changedListenerMap.remove(k);
        }
    }


    private static void onKeyChanged(String namespace, String key, ConfigChange configValue){
        String k  = namespace + "." + key;
        if(changedListenerMap.containsKey(k)){

            KeyChangedListener listener = changedListenerMap.get(k);
            // 如果 changedListenerMap 存在且值是null,则删除节点
            if(listener == null){
                changedListenerMap.remove(k);
                return;
            }

            lock.lock();
            try{
                logger.info("KEY:{}的新值:{}",k,configValue.getNewValue());
                listener.onKeyChanged(namespace, key, configValue.getNewValue(), configValue.getOldValue());
            } catch(Exception e){
                e.printStackTrace();
            } finally {
                lock.unlock();
            }

        }
    }

    public interface KeyChangedListener {
        void onKeyChanged(String namespace, String key, String newValue, String oldValue);
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        //System.out.println(ConfigProxy.getNamespaces());
        //Config config = ConfigService.getConfig("123");
        //System.out.println(ConfigService.getAppConfig());
        //System.out.println(System.getProperty("namespace"));
        // 获取指定namespace的配置
        //System.out.println(entry);
        //String entry = ConfigProxy.getQuoteIteraValue("test","${a}");
        //System.out.println(entry);

        //while(true)
        //System.out.println(ConfigService.getConfig("ccinfra-restapi-rc").getProperty("a", null));
       /* long startTime = System.currentTimeMillis();
        long count = 0;
        while(System.currentTimeMillis() - startTime < 1 * 1000) {
            String bootstrapServers = ConfigProxy.getProperty("test123", "a");
            count ++;
            System.out.println(bootstrapServers);
        }
        System.out.println(count);*/
        //String bootstrapServers = ConfigProxy.getProperty("test", "a");
        String bootstrapServers = ConfigProxy.getProperty("DownStream.kafka.bootstrap");
        System.out.println(bootstrapServers);
       /* for (Map.Entry<KeyValue, Set<KeyValue>> entry : relationNodes.entrySet()) {
            System.out.println(entry.getKey().toString());
            for (KeyValue e : entry.getValue()) {
                System.out.println("--->" + e.toString());
            }
        }*/
        //TimeUnit.MINUTES.sleep(Integer.MAX_VALUE);
        //logger.info("123123");
        //System.out.println( System.getProperty("env"));
        //String min = ConfigProxy.getLocalProperty("dev.properties", "filter.mysql.url");
        //System.out.println(min);
        //Thread.sleep(Integer.MAX_VALUE);
        //Map<String, String> map = ConfigProxy.getPropertyByPre("ccinfra-mqtt", "dynamicDataTypeToTopicSuffixMapping");
        //map.keySet().forEach(key -> {
        //    System.out.println(map.get(key));
        //});
    }
}
