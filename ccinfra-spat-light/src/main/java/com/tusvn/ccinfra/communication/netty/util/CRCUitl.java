package com.tusvn.ccinfra.communication.netty.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

public class CRCUitl {

	/**
	 * * CRC16_CCITT：多项式x16+x12+x5+1（0x1021），初始值0x0000，低位在前，高位在后，结果与0x0000异或 *
	 * 0x8408是0x1021按位颠倒后的结果。 * @param buffer * @return
	 */
	public static int CRC16_CCITT(byte[] buffer) {
		int wCRCin = 0x0000;
		int wCPoly = 0x8408;
		for (byte b : buffer) {
			wCRCin ^= ((int) b & 0x00ff);
			for (int j = 0; j < 8; j++) {
				if ((wCRCin & 0x0001) != 0) {
					wCRCin >>= 1;
					wCRCin ^= wCPoly;
				} else {
					wCRCin >>= 1;
				}
			}
		}
		return wCRCin ^= 0x0000;
	}

	/**
	 * * CRC-CCITT (0xFFFF) *
	 * CRC16_CCITT_FALSE：多项式x16+x12+x5+1（0x1021），初始值0xFFFF，低位在后，高位在前，结果与0x0000异或
	 * * * @param buffer * @return
	 */
	public static int CRC16_CCITT_FALSE(byte[] buffer) {
		int wCRCin = 0xffff;
		int wCPoly = 0x1021;
		for (byte b : buffer) {
			for (int i = 0; i < 8; i++) {
				boolean bit = ((b >> (7 - i) & 1) == 1);
				boolean c15 = ((wCRCin >> 15 & 1) == 1);
				wCRCin <<= 1;
				if (c15 ^ bit)
					wCRCin ^= wCPoly;
			}
		}
		wCRCin &= 0xffff;
		return wCRCin ^= 0x0000;
	}

	/**
	 * * CRC-CCITT (XModem) *
	 * CRC16_XMODEM：多项式x16+x12+x5+1（0x1021），初始值0x0000，低位在后，高位在前，结果与0x0000异或 *
	 * * @param buffer * @return
	 */
	public static int CRC16_XMODEM(byte[] buffer) {
		int wCRCin = 0x0000; // initial value 65535
		int wCPoly = 0x1021; // 0001 0000 0010 0001 (0, 5, 12)
		for (byte b : buffer) {
			for (int i = 0; i < 8; i++) {
				boolean bit = ((b >> (7 - i) & 1) == 1);
				boolean c15 = ((wCRCin >> 15 & 1) == 1);
				wCRCin <<= 1;
				if (c15 ^ bit)
					wCRCin ^= wCPoly;
			}
		}
		wCRCin &= 0xffff;
		return wCRCin ^= 0x0000;
	}

	/**
	 * * CRC16_X25：多项式x16+x12+x5+1（0x1021），初始值0xffff，低位在前，高位在后，结果与0xFFFF异或 *
	 * 0x8408是0x1021按位颠倒后的结果。 * @param buffer * @return
	 */
	public static int CRC16_X25(byte[] buffer) {
		int wCRCin = 0xffff;
		int wCPoly = 0x8408;
		for (byte b : buffer) {
			wCRCin ^= ((int) b & 0x00ff);
			for (int j = 0; j < 8; j++) {
				if ((wCRCin & 0x0001) != 0) {
					wCRCin >>= 1;
					wCRCin ^= wCPoly;
				} else {
					wCRCin >>= 1;
				}
			}
		}
		return wCRCin ^= 0xffff;
	}

	static int int_2_unsignedbyte(int v) {
		return v & 0xff;
	}

	static int int_2_unignedshort(int v) {
		return v & 0xffff;
	}

	static int gen_crs(byte[] data) {
		int c = 0; // unsigned char
		int treat = 0;// unsigned char
		int bcrc = 0; // unsigned char
		int wcrc = 0; // unsigned short
		int i = 0;
		int j = 0;

		for (i = 0; i < data.length; i++) {
			c = int_2_unsignedbyte(data[i]);

			for (j = 0; j < 8; j++) {
				treat = c & 0x80;
				c <<= 1;
				bcrc = (wcrc >> 8) & 0x80;

				wcrc <<= 1;
				wcrc = int_2_unignedshort(wcrc);
				if (treat != bcrc)
					wcrc ^= 0x1021;
			}
		}

		return int_2_unignedshort(wcrc);
	}
	
	
	/////////---------------
	/**
     * CRC16_USB：多项式x16+x15+x2+1（0x8005），初始值0xFFFF，低位在前，高位在后，结果与0xFFFF异或
     * 0xA001是0x8005按位颠倒后的结果
     * @param buffer
     * @return
     */
    public static int CRC16_USB(byte[] buffer) {
        int wCRCin = 0xFFFF;
        int wCPoly = 0xa001;
        for (byte b : buffer) {
            wCRCin ^= ((int) b & 0x00ff);
            for (int j = 0; j < 8; j++) {
                if ((wCRCin & 0x0001) != 0) {
                    wCRCin >>= 1;
                    wCRCin ^= wCPoly;
                } else {
                    wCRCin >>= 1;
                }
            }
        }
        return wCRCin ^= 0xffff;
    }
    
    /**
     * CRC-16 (Modbus)
     * CRC16_MODBUS：多项式x16+x15+x2+1（0x8005），初始值0xFFFF，低位在前，高位在后，结果与0x0000异或
     * 0xA001是0x8005按位颠倒后的结果
     * @param buffer
     * @return
     */
    public static int CRC16_MODBUS(byte[] buffer) {
        int wCRCin = 0xffff;
        int POLYNOMIAL = 0xa001;
        for (byte b : buffer) {
            wCRCin ^= ((int) b & 0x00ff);
            for (int j = 0; j < 8; j++) {
                if ((wCRCin & 0x0001) != 0) {
                    wCRCin >>= 1;
                    wCRCin ^= POLYNOMIAL;
                } else {
                    wCRCin >>= 1;
                }
            }
        }
        return wCRCin ^= 0x0000;
    }
    
    /******
     * 
     * 
CRC-16 / MODBUS ：

1）CRC寄存器初始值为 FFFF；即16个字节全为1；

2）CRC-16 / MODBUS的多项式A001H (1010 0000 0000 0001B) ‘H’表示16进制数，‘B’表示二进制数

计算步骤为： 
(1).预置 16 位寄存器为十六进制 FFFF（即全为 1） ，称此寄存器为 CRC  寄存器；  
(2).把第一个 8  位数据与 16  位 CRC  寄存器的低位相异或，把结果放于 CRC  寄
存器； 

(3).检测相异或后的CRC寄存器的最低位，若最低位为1：CRC寄存器先右移1位，再与多项式A001H进行异或；若为0，则CRC寄存器右移1位，无需与多项式进行异或。

(4).重复步骤 3  ，直到右移 8  次，这样整个 8 位数据全部进行了处理； 
(5).重复步骤 2  到步骤4，进行下一个 8  位数据的处理； 
(6).最后得到的 CRC  寄存器即为 CRC 码。 

     * 
     * 
     * */
    
    public static int test_CRC16_MODBUS(byte[] buffer) {
        int wCRCin = 0xffff;
        int POLYNOMIAL = 0xA001;
        for (byte b : buffer) {
        	
            wCRCin ^= ((int) b & 0x00ff);
            ////wCRCin = ((int) b & 0x00ff);
 
            for (int j = 0; j < 8; j++) {
                if ((wCRCin & 0x0001) != 0) {  ////////判断右移出的是不是1，如果是1则与多项式进行异或。
                    wCRCin >>= 1;              ////////先将数据右移一位
                    wCRCin ^= POLYNOMIAL;      ////////与上面的多项式进行异或
                } else {              
                    wCRCin >>= 1;  ////如果不是1，则直接移出
                } 
            }
        }
        
        return ~wCRCin;
        ////return wCRCin ^= 0x0000;

    }
    
 
 
    
    
    public static int CRC16_GAT_1(byte [] data)
    {
        int   wCRCin = 0xFFFF;
        int   wCPoly = 0x8005;
 
        byte wChar=0;
        for (int x=0;x<data.length;x++)
        {
 
        	wChar=data[x];
        	
        	/////System.out.printf("===>x=%d, wChar=%x \n", x, wChar);
    		wCRCin ^= (   wChar  << 8);
    		
    		////System.out.printf("wChar=%x \n",wChar);

 
            for(int i = 0; i < 8; i++)
            {
                if((wCRCin & 0x8000)!=0)
                {
                    wCRCin = (wCRCin << 1) ^ wCPoly;
                }
                else
                {
                    wCRCin = wCRCin << 1;
                }
            }
        }
     
        
    	return (wCRCin& 0x0000ffff) ;  /////只取低的2个字节
        
        ////return intToBytes((wCRCin& 0x0000ffff));
    }
 
    ////////////将int转换成byte数组，低位在前，高位在后
    private static byte[] intToBytes(int value) {
        byte[] src = new byte[2];
        src[1] = (byte) ((value >> 8) & 0xFF);
        src[0] = (byte) (value & 0xFF);
        return src;
    }
    
    
//    public static int twoBytes2Int(byte[] bytes) { 
//    	int off=0;
//        int b2 = bytes[off]     & 0xFF; 
//        int b1 = bytes[off + 1] & 0xFF; 
//        ////int b0 = bytes[off + 2] & 0xFF; 
//        ////int b3 = bytes[off + 3] & 0xFF; 
////        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3; 
//        
//        return   (b1 << 8) | (b2)  ; 
//      }
    
    
    public static int changeHighAndLowForInt(int value)
    {
    	byte[] dest_arr=intToBytes(value); /////
    	
    	
    	ParseUtils.printByteArray("bbbbbb", dest_arr);
    	
    	
//    	ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(dest_arr.length);
//		
//		buf.writeBytes(dest_arr);
		
		//int dest=buf.getUnsignedShortLE(0);
    	
    	return 0;
    }
    
    
    public static int FindCRC(byte[] data){
        int CRC=0xffff;
        
        ////int genPoly =0x8C;
        int genPoly=0xa001;
        
        for(int i=0;i<data.length; i++){
        	CRC =data[i] & 0x00ff;
        
            ///CRC &=0xff;//保证CRC余码输出为1字节。
        
            for(int j=0;j<8;j++){
                if((CRC& 0x01) != 0){
                     CRC =(CRC >> 1) ^ genPoly;
        
                     ////CRC&= 0xff;//保证CRC余码输出为1字节。
        
                 }else{
                     CRC>>= 1;
        
                 }
        
              }
        
        }
        
        ///CRC &= 0xff;//保证CRC余码输出为1字节。
        
        return CRC;
  }
    
    
    
	public static int test_CRC16_X25(byte[] buffer) {
		int wCRCin = 0xffff;
		int wCPoly = 0xa001;
		for (byte b : buffer) {
			wCRCin ^= ((int) b & 0x00ff);
			for (int j = 0; j < 8; j++) {
				if ((wCRCin & 0x0001) != 0) {
					wCRCin >>= 1;
					wCRCin ^= wCPoly;
				} else {
					wCRCin >>= 1;
				}
			}
		}
		return wCRCin ^= 0xffff;
	}
    

	public static void main(String[] args) {
//		String str = "303130";
//		byte[] data = ByteBufUtil.decodeHexDump("303130");
//		System.out.println(CRC16_CCITT_FALSE(data));
//		System.out.println(CRC16_CCITT(data));
//		System.out.println(CRC16_X25(data));
//		System.out.println(CRC16_XMODEM(data));
//		System.out.println(gen_crs(data));
		
		
		String inStr="040023AE010010FFFF23AE01002003002C91A36000003C1083030100000000590000000000000000000000040000040104165E0204165E0307165E0808253A5A0004050225130601253A0701253A0C08165EB400040904165E0A04165E0B07165E1008161B0E01040408165E0D0216420E01161B0F01161B";
//		
		////String inStr="0401";
		
//		////byte[] data = ByteBufUtil.decodeHexDump(inStr);
//		
		byte[] data = com.alibaba.druid.util.HexBin.decode(inStr);
		ParseUtils.printByteArray("aaaaa", data);
//		
       ////int n=test_CRC16_MODBUS(data);
		int n=CRC16_GAT_1( data);
//		////int n=FindCRC(data);
		System.out.println("===>n="+n);
		System.out.printf("=======>%x",n);
		
		int n2=changeHighAndLowForInt(n);
		
		System.out.println("===>n2="+n2);
		System.out.printf("n2222=%x",n2);
		
//		byte b=0x2;
//		byte d=InvertUint8(b);
//		System.out.printf("%x",d);
		
	}
}
