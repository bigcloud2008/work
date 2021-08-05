package com.tusvn.ccinfra.config.uitls;

import com.ctrip.framework.apollo.ConfigService;
import com.tusvn.ccinfra.config.ConfigProxy;
import com.tusvn.ccinfra.config.backtype.ApolloEntry;
import com.tusvn.ccinfra.config.backtype.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Auther: 赵云海
 * @Date: 2019/4/23 15:25
 * @Version: 1.0
 * @Description: TODO
 */
public class UtilsCommons {
    /**
     * 格式化引用的Pattern值
     */
    static final Pattern p = Pattern.compile("(\\$\\{)(.*?)(\\})");

    /**
     * 读取getProperties文件
     */
    public static Properties getProperties(String path) {
        Properties properties = new Properties();
        ClassLoader loader = UtilsCommons.class.getClassLoader();
        InputStream inputstream = loader != null ? loader.getResourceAsStream(path) : ClassLoader.getSystemResourceAsStream(path);
        try {
            properties.load(inputstream);
        } catch (IOException e) {
            logger.error("init properties <" + path + "> exception : " + e, e);
        }
        return properties;
    }


    public static String find(String namespace, String key, String quote) {
        ConfigProxy.addChangeListener(namespace);
        StringBuffer buffer = new StringBuffer();
        Matcher matcher = p.matcher(quote);
        while (matcher.find()) {
            addParentNode(namespace, key, matcher.group());
            ApolloEntry entry = getQuoteValue(namespace, matcher.group());
            if (entry != null && p.matcher(entry.getValue()).find()) {
                entry.setValue(find(entry.getNamespace(), entry.getKey(), entry.getValue()));
            }
            if (entry == null) {
                throw new NullPointerException();
            } else {
                matcher.appendReplacement(buffer, entry.getValue());
            }
        }
        return matcher.appendTail(buffer).toString();
    }

    /**
     * 添加关联key信息
     *
     * @param namespace
     * @param key
     * @param quote
     */
    public static void addParentNode(String namespace, String key, String quote) {
        String _key = quote.substring(2, quote.length() - 1);
        ConfigProxy.addParentNode(new KeyValue(namespace, _key), new KeyValue(namespace, key));
        String[] keys = _key.split("\\.");
        if (keys.length >= 2) {
            _key = _key.replace(keys[0] + ".", "");
            ConfigProxy.addParentNode(new KeyValue(keys[0], _key), new KeyValue(namespace, key));
        }
    }


    /**
     * 获取引用的真实值
     *
     * @param namespace
     * @param quote
     * @return 引用工作空间及引用真值
     */
    public static ApolloEntry getQuoteValue(String namespace, String quote) {
        String k = quote.substring(2, quote.length() - 1);
        String v = ConfigService.getConfig(namespace).getProperty(k, null);
        if (v != null) {
            return new ApolloEntry(namespace, k, v);
        }
        String[] keys = k.split("\\.");
        if (keys.length < 2) {
            throw new NullPointerException();
        }
        List<String> namespaces = ConfigProxy.getNamespaces();
        if (!namespaces.contains(keys[0])) {
            throw new NullPointerException();
        }
        k = k.replace(keys[0] + ".", "");
        v = ConfigService.getConfig(keys[0]).getProperty(k, null);
        return v == null ? null : new ApolloEntry(keys[0], k, v);
    }

    private static final Logger logger = LoggerFactory.getLogger(UtilsCommons.class);
}
