package com.sinobbd.data.merge;

import java.security.NoSuchAlgorithmException;

public class CommonTools {

	public static long ipToLong(String strIp) {
		long[] ip = new long[4];
		// 先找到IP地址字符串中.的位置
		int position1 = strIp.indexOf(".");
		int position2 = strIp.indexOf(".", position1 + 1);
		int position3 = strIp.indexOf(".", position2 + 1);
		// 将每个.之间的字符串转换成整型
		ip[0] = Long.parseLong(strIp.substring(0, position1));
		ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
		ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
		ip[3] = Long.parseLong(strIp.substring(position3 + 1));
		return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
	}

	// 将十进制整数形式转换成127.0.0.1形式的ip地址
	public static String longToIP(long longIp) {
		StringBuffer sb = new StringBuffer("");
		// 直接右移24位
		sb.append(String.valueOf((longIp >>> 24)));
		sb.append(".");
		// 将高8位置0，然后右移16位
		sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
		sb.append(".");
		// 将高16位置0，然后右移8位
		sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
		sb.append(".");
		// 将高24位置0
		sb.append(String.valueOf((longIp & 0x000000FF)));
		return sb.toString();
	}

	public static int ipToInt(String strIp) {
		String[] ss = strIp.split("\\.");
		if (ss.length != 4) {
			throw new RuntimeException("valid ip error: " + strIp);
		}
		byte[] bytes = new byte[4];
		for (int i = 0; i < 4; i++) {
			bytes[i] = (byte) Integer.parseInt(ss[i]);
		}
		return byteToInt(bytes);
	}

	public static int byteToInt(byte[] bytes) {
		int num = 0;
		num += (bytes[3] & 0x000000FF);
		num += (bytes[2] & 0x000000FF) << (8);
		num += (bytes[1] & 0x000000FF) << (16);
		num += (bytes[0] & 0x000000FF) << (24);
		return num;
	}

	public static byte[] intToByte(int num) {
		byte[] bytes = new byte[4];
		bytes[0] = (byte) ((num >> 24) & 0xff);
		bytes[1] = (byte) ((num >> 16) & 0xff);
		bytes[2] = (byte) ((num >> 8) & 0xff);
		bytes[3] = (byte) (num & 0xff);
		return bytes;
	}

	public static String md5(String source) {
		return md5(source.getBytes());
	}

	public static String md5(byte[] source) {
		String s = null;
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
		try {
			java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
			md.update(source);
			byte tmp[] = md.digest();// MD5 的计算结果是一个 128 位的长整数，
			// 用字节表示就是 16 个字节
			char str[] = new char[16 * 2];// 每个字节用 16 进制表示的话，使用两个字符， 所以表示成 16
			// 进制需要 32 个字符
			int k = 0;// 表示转换结果中对应的字符位置
			for (int i = 0; i < 16; i++) {// 从第一个字节开始，对 MD5 的每一个字节// 转换成 16
				// 进制字符的转换
				byte byte0 = tmp[i];// 取第 i 个字节
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];// 取字节中高 4 位的数字转换,// >>>
				// 为逻辑右移，将符号位一起右移
				str[k++] = hexDigits[byte0 & 0xf];// 取字节中低 4 位的数字转换

			}
			s = new String(str);// 换后的结果转换为字符串

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return s;
	}

	public static Integer parseInt(Object o, int defaultValue) {
		try {
			int intValue = Integer.parseInt(o.toString());
			return intValue;
		} catch (NumberFormatException e) {
			e.printStackTrace();
			return defaultValue;
		}
	}

	public static String parseString(Object o) {
		try {
			String value = o.toString();
			return value;
		} catch (Exception e) {
			e.printStackTrace();
			return "0";
		}
	}

	public static double AddTwoDouble(Object d1, Object d2) {

		double db1 = 0;
		double db2 = 0;
		if (d1 != null) {
			db1 = Double.valueOf(d1.toString());
		}
		if (d2 != null) {
			db2 = Double.valueOf(d2.toString());
		}
		double value = db1 + db2;
		return value;

	}

}
