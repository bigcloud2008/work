/**
 * 
 */
package com.tusvn.ccinfra.api.utils;

/**
 * Kafka 数据消费位置枚举
 * @author xchao on 2018/12/14.
 */
public enum OffsetReset {

    /**
     * earliest， 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费 
     */
    EARLIEST(1, "earliest", "当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费 "),
    /**
     * latest，当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据 
     */
    LATEST(  2, "latest", "当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据 "),
    /**
     * none，topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
     */
    NONE(    3, "none", "topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常"),
    ;
    private int level = 1;
    private String label = "";
    private String desc = "";
    
    /**
     * @param level
     * @param label
     * @param desc
     */
    private OffsetReset(int level, String label, String desc) {
        this.level = level;
        this.label = label;
        this.desc = desc;
    }

    /**
     * @return the level
     */
    public int getLevel() {
        return level;
    }

//    /**
//     * @param level the level to set
//     */
//    public void setLevel(int level) {
//        this.level = level;
//    }

    /**
     * @return the label
     */
    public String getLabel() {
        return label;
    }

//    /**
//     * @param label the label to set
//     */
//    public void setLabel(String label) {
//        this.label = label;
//    }

    /**
     * @return the desc
     */
    public String getDesc() {
        return desc;
    }

//    /**
//     * @param desc the desc to set
//     */
//    public void setDesc(String desc) {
//        this.desc = desc;
//    }
//    
}
