package com.sinobbd.strider.dota;

import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateFormatBBD {
	private static DateTimeFormatter formcn;
	private static DateTimeFormatter formym;
	private static DateTimeFormatter formymdhm;
	private static DateTimeFormatter formYMDNoDot;
	private static DateTimeFormatter formymNoDot;
	static {		
		formcn = DateTimeFormat.forPattern("yyyy.MM.dd").withLocale(Locale.CHINA);
		formYMDNoDot = DateTimeFormat.forPattern("yyyyMMdd").withLocale(Locale.CHINA);
		formymNoDot = DateTimeFormat.forPattern("yyyyMM").withLocale(Locale.CHINA);
		formym = DateTimeFormat.forPattern("yyyy.MM").withLocale(Locale.CHINA);
		formymdhm = DateTimeFormat.forPattern("yyyyMMddHHmm").withLocale(Locale.CHINA);

	}
	
	public static DateTime convertNoDotToDateTime(String strdate){
		DateTime dt=formYMDNoDot.parseDateTime(strdate);
		return dt;
	}
	
	public static DateTime convertNoDotToDateTimeMin(String strdate){
		DateTime dt=formymdhm.parseDateTime(strdate);
		return dt;
	}

	public static String convertToStr(Double sdate) throws Exception {
		Long time=Math.round((sdate*1000));
		DateTime dt=new DateTime(time);
		String tm=dt.toString(formcn);
		return tm;
	}
	public static String convertToStrYM(Double sdate) throws Exception {
		Long time=Math.round((sdate*1000));
		DateTime dt=new DateTime(time);
		String tm=dt.toString(formym);
		return tm;
	}
	
	public static String convertMillsToStrYM(Double sdate) throws Exception {
		Long time=Math.round((sdate));
		DateTime dt=new DateTime(time);
		String tm=dt.toString(formym);
		return tm;
	}
	
	public static String convertToStr(String srtdate) throws Exception {
		double sdate=new Double(srtdate);
		Long time=Math.round((sdate*1000));
		DateTime dt=new DateTime(time);
		String tm=dt.toString(formcn);
		return tm;
	}
	
	public static String convertToStrYM(String srtdate) throws Exception {
		double sdate=new Double(srtdate);
		Long time=Math.round((sdate*1000));
		DateTime dt=new DateTime(time);
		String tm=dt.toString(formym);
		return tm;
	}
	public static String convertToStrYMDHM(Long timeMill) throws Exception {		
		DateTime dt=new DateTime(timeMill);
		String tm=dt.toString(formymdhm);
		return tm;
	}
	
	public static String convertToStrYMDNODot(Long timeMill) throws Exception {		
		DateTime dt=new DateTime(timeMill);
		String tm=dt.toString(formYMDNoDot);
		return tm;
	}
	
	public static String convertToStrYM_NoDot(Long timeMill) throws Exception {		
		DateTime dt=new DateTime(timeMill);
		String tm=dt.toString(formymNoDot);
		return tm;
	}
	
	public static Long convertYMDHMtoMill(String yyyyMMddHHmm) throws Exception {		
		DateTime dt=formymdhm.parseDateTime(yyyyMMddHHmm);
		Long timeMill=dt.getMillis();	
		return timeMill;
	}

	public static void main(String[] args) throws Exception {
		//11/十二月/2016:08:01:02 +0800
//		String source ="22/Nov/2016:16:45:00 +0800";// new String("22/Nov/2016:16:45:00 +0800".getBytes(), "UTF-8");// "05/Nov/2016:00:02:33 +0800";//
//		// String source ="2016-05-01 12:22:10" ;
//		System.out.println("----time---"+String.valueOf(System.currentTimeMillis()));
//		try {
//			List<String> alres = parseToUs(source);
//			// // 计算表
//			// String tableName = fix;
//			System.out.println(alres.get(0) + ":---------" + alres.get(1));
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
		
		

//		 String dateStr = "01/Dec/2016:00:00:01 +0800";
//		 String pattern = "EEE, dd-MMM-yyyy HH:mm:ss z";
////		 DateFormat format = new SimpleDateFormat(pattern, Locale.US);
//		DateTime tm= form.parseDateTime(dateStr);
//		
		//System.out.println(tm);
		 
	}

}
