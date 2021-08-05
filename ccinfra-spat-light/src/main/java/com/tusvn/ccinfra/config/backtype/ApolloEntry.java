package com.tusvn.ccinfra.config.backtype;

import java.util.Objects;

/**
 * @Auther: 赵云海
 * @Date: 2019/5/27 14:01
 * @Version: 1.0
 * @Description: TODO
 */
public class ApolloEntry {
    private String namespace;
    private String key;
    private String value;

    public ApolloEntry(String namespace, String key, String value) {
        this.namespace = namespace;
        this.key = key;
        this.value = value;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public KeyValue toKeyValue(){
        return new KeyValue(namespace,key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApolloEntry keyValue = (ApolloEntry) o;
        return Objects.equals(namespace, keyValue.namespace) &&
                Objects.equals(key, keyValue.key) &&
                Objects.equals(value, keyValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, key, value);
    }

    @Override
    public String toString() {
        return "ApolloEntry{" +
                "namespace='" + namespace + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
