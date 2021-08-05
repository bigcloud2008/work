package com.tusvn.ccinfra.communication.netty.util;

import java.math.BigInteger;

public class ParseUtils {

	public static void printByteArray(String title, byte[] mem_buf) {
		////////////////// 把VC返回的每一个字节打出来
		System.out.println("=======================>from [" + title + "] bytes: ");
		for (int i = 0; i < mem_buf.length; i++) {
			byte b[] = new byte[1];
			b[0] = mem_buf[i];
			BigInteger a = new BigInteger(1, b);

			String s = a.toString(16);
			if (s.length() < 2)
				s = "0" + s;
			System.out.print(s + " ");
		}
		System.out.println("\n=======================>end print-----------------");
	}

	public static String printByteArrayToFile(byte[] mem_buf) {
		StringBuffer buff = new StringBuffer();
		////////////////// 把VC返回的每一个字节打出来

		for (int i = 0; i < mem_buf.length; i++) {
			byte b[] = new byte[1];
			b[0] = mem_buf[i];
			BigInteger a = new BigInteger(1, b);

			String s = a.toString(16);
			if (s.length() < 2)
				s = "0" + s;
			buff.append(s + " ");
		}

		return buff.toString();
	}

	private static byte charToByte(char c) {

		return (byte) ("0123456789ABCDEF".indexOf(c));

	}

	public static byte[] hexStringToBytes(String hexString) {

		if (hexString == null || hexString.equals("")) {
			return null;
		}

		hexString = hexString.toUpperCase();

		int length = hexString.length() / 2;

		char[] hexChars = hexString.toCharArray();

		byte[] d = new byte[length];

		for (int i = 0; i < length; i++) {

			int pos = i * 2;

			d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
		}

		return d;
	}

}
