package com.tusvn.ccinfra.communication.netty.model;

/**
 * 信号灯编码颜色映射
 * @author tsing
 *
 */
public enum SpatColor {
	
	
	GREEN("green", 1,5){
		@Override
        public SpatColor getNext() {
            return GREENBLINK;
        }
	}, 
	GREENBLINK("greenblink", 2,6) {
		@Override
		public SpatColor getNext() {
			return YELLOW;
		}
	}, 
	RED("red", 3,3) {
		@Override
		public SpatColor getNext() {
			return GREEN;
		}
	}, 
	REDBLINK("redblink", 6,2) {
		@Override
		public SpatColor getNext() {
			return null;
		}
	},
	YELLOW("yellow", 4,7) {
		@Override
		public SpatColor getNext() {
			return YELLOWBLINK;
		}
	}, 
	YELLOWBLINK("yellowblink", 5,8) {
		@Override
		public SpatColor getNext() {
			return RED;
		}
	};
	
	// 成员变量  
    private String name;  
    private int srcCode; 
    private int desCode; 
    
    public abstract SpatColor getNext();
    
    // 构造方法  
    private SpatColor(String name, int srcCode,int desCode) {  
        this.name = name;  
        this.srcCode = srcCode;  
        this.desCode = desCode;
    }  
    
    // 普通方法  
    public static SpatColor getSpatColor(int code) {
    	for (SpatColor c : SpatColor.values()) {  
            if (c.getSrcCode() == code) {  
                return c;  
            }  
        }  
        return null;  
    }
    
    // 普通方法  
    public static String getName(int code) {  
        for (SpatColor c : SpatColor.values()) {  
            if (c.getSrcCode() == code) {  
                return c.name;  
            }  
        }  
        return null;  
    }
    
    // 普通方法  
    public static int getDesCode(int code) {  
        for (SpatColor c : SpatColor.values()) {  
            if (c.getSrcCode() == code) {  
                return c.desCode;  
            }  
        }  
        return -1;  
    }
    

    public String getName() {  
        return name;  
    }  
    public void setName(String name) {  
        this.name = name;  
    }

	public int getSrcCode() {
		return srcCode;
	}

	public void setSrcCode(int srcCode) {
		this.srcCode = srcCode;
	}

	public int getDesCode() {
		return desCode;
	}

	public void setDesCode(int desCode) {
		this.desCode = desCode;
	}  
    
}
