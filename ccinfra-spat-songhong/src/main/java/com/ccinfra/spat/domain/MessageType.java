/**
 * 
 */
package com.ccinfra.spat.domain;

/**
 * 自定义扩展消息类型
 * @author xchao on 2019/03/26.
 */
public enum MessageType {
    /**
     * 0x87, 红绿灯
     */
    SPAT(0x87, "红绿灯"),
    /**
     * 0x0C，心跳检测
     */
    PINGREQ(0x0C, "心跳请求"),
    /**
     * 0x0D，心跳检测
     */
    PINGRESP (0x0D, "心跳响应"),
    /**
     * 0x05，RCU侧检测目标结果
     */
    DETECTIONS(0x05, "RCU侧检测目标结果"),
    /**
     * 0x99，未知
     */
    UNKONW(0xFE, "未知类型")
    ;
    private int intVal = 0x01;
    private String desc = "";
    private MessageType(int intVal, String desc) {
        this.intVal = intVal;
        this.desc = desc;
    }
    /**
     * @return the intVal
     */
    public int getIntVal() {
        return intVal;
    }
    /**
     * @param intVal the intVal to set
     */
    public void setIntVal(int intVal) {
        this.intVal = intVal;
    }
    /**
     * @return the desc
     */
    public String getDesc() {
        return desc;
    }
    /**
     * @param desc the desc to set
     */
    public void setDesc(String desc) {
        this.desc = desc;
    }
    private static MessageType[] messageTypes = MessageType.values();
    /**
     * 根据 intVal 获取对应的枚举对象 {@link MessageType}
     * @param intVal 枚举对象的 intVal
     * @return 对应的枚举对象 {@link MessageType}
     */
    public static MessageType getByIntVal(int intVal) {
        for(MessageType msgType : messageTypes) {
            if(intVal == msgType.getIntVal()) {
                return msgType;
            }
        }
        return MessageType.UNKONW;
    }
}
