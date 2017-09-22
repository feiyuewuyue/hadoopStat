/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.sinobbd.data.merge;

/**
 * modified to fit ES5.X  SINOBBD 修改
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sinobbd.kafka.process.FieldConst;


/**
 */
public class ParseSrcData {

	public static final Logger logger = LoggerFactory.getLogger(ParseSrcData.class);
	public static final String SEPARATOR = "@##@";
	public static final String HBASE_SEPATOR = "\" \"";  //每个字段用引号引起了，并用空格分隔
	//替换为LogALLFieldsFromKafka
	@Deprecated
	public static final String[] FusionDetailFIELDS = { "firm_name", "response_time", "http_range","upstream_response_time",
			 "time_local", "time_unix", "clientip", "method", "domain", "request", "protocol","http_status",
			 "body_bytes_sent", "referer", "user_agent", "hit_status_detail", "hit_status","upstream_addr",
			 "server_addr", "X_Info_Fetcher", "X_Info_ObjSize", "X_Info_request_id", "X_Info_MD5","prov", "country", 
			"city", "ISP", "latitude", "longitude", "blockid", "referer_host", "search_engine","hackerid", "layer",
			 "def1", "file_name", "line_num", "rawdata", "except_desc" };
	//替换为BandCommonFieldsFromKafka	
	@Deprecated
	public static final String[] FusionBandFIELDS = { "firm_name", "response_time", "http_range","upstream_response_time",
			 "time_local", "time_unix", "clientip", "method", "domain", "request", "protocol","http_status",
			 "body_bytes_sent", "referer", "user_agent", "hit_status_detail", "hit_status","upstream_addr",
			 "server_addr", "X_Info_Fetcher", "X_Info_ObjSize", "X_Info_request_id", "X_Info_MD5","prov", "country", 
			"city", "ISP", "latitude", "longitude", "blockid", "referer_host", "search_engine","hackerid", "layer",
			 "def1", "file_name", "line_num" };
	
	
	//替换为BandCommonFieldsFromKafka	 
	 @Deprecated
	 public static final String[] jxBandFiledsArray ={"firm_name", "response_time", "http_range", "p_response_time",
			"time_local", "time_unix", "clientip", "method", "domain", "request", "protocol", "http_status",
			"body_bytes_sent", "referer", "user_agent", "hit_status_detail", "hit_status", "upstream_addr",
			"server_addr", "X_Info_Fetcher", "X_Info_ObjSize", "X_Info_request_id", "X_Info_MD5", "prov", "country",
			"city", "ISP", "latitude", "longitude", "blockid", "referer_host", "search_engine", "hackerid", "layer",
			"def1", "hit_status_whole", "host", "p_response_length", "p_server", "p_status", "request_from", "ruleid",
			"xff", "rawdata", "except_desc" };
	 
	//替换为LogALLFieldsFromKafka
	 @Deprecated
		public static final String[] jxDetailFiledsArray ={"firm_name", "response_time", "http_range", "p_response_time",
				"time_local", "time_unix", "clientip", "method", "domain", "request", "protocol", "http_status",
				"body_bytes_sent", "referer", "user_agent", "hit_status_detail", "hit_status", "upstream_addr",
				"server_addr", "X_Info_Fetcher", "X_Info_ObjSize", "X_Info_request_id", "X_Info_MD5", "prov", "country",
				"city", "ISP", "latitude", "longitude", "blockid", "referer_host", "search_engine", "hackerid", "layer",
				"def1", "hit_status_whole", "host", "p_response_length", "p_server", "p_status", "request_from", "ruleid",
				"xff", "rawdata", "except_desc" };
	 
	 
	 public static final String[] BandCommonFieldsFromKafka={"firm_name","response_time","http_range","p_response_time", 
	       "time_local","time_unix","clientip","method","domain","request","protocol","http_status",
	       "body_bytes_sent","referer","user_agent","hit_status_detail","hit_status","upstream_addr",
	       "server_addr","X_Info_Fetcher","X_Info_ObjSize","X_Info_request_id","X_Info_MD5","prov","country",
	       "city","ISP","latitude","longitude","blockid","referer_host","search_engine","hackerid","layer",
	       "def1", "file_name", "hit_status_whole", "host"};
	
	  public static final String[] LogALLFieldsFromKafka= {"firm_name","response_time","http_range","p_response_time", 
		  "time_local","time_unix","clientip","method","domain","request","protocol","http_status",
		  "body_bytes_sent","referer","user_agent","hit_status_detail","hit_status","upstream_addr",
		  "server_addr","X_Info_Fetcher","X_Info_ObjSize","X_Info_request_id","X_Info_MD5","prov","country",
		  "city","ISP","latitude","longitude","blockid","referer_host","search_engine","hackerid","layer",
		  "def1", "file_name", "line_num", "hit_status_whole", "host", "p_response_length","p_server","p_status","request_from","ruleid","xff", "peak"};


	
	//替换为LogALLFieldsFromKafka
	  @Deprecated 
	public static final String[] FIELDS_Common = { "firm_name", "response_time", "http_range","upstream_response_time",
		 "time_local", "time_unix", "clientip", "method", "domain", "request", "protocol","http_status",
		 "body_bytes_sent", "referer", "user_agent", "hit_status_detail", "hit_status","upstream_addr",
		 "server_addr", "X_Info_Fetcher", "X_Info_ObjSize", "X_Info_request_id", "X_Info_MD5","prov", "country", 
		"city", "ISP", "latitude", "longitude", "blockid", "referer_host", "search_engine","hackerid", "layer"};

	/**JX和融合CDN日志，此列字段顺序一致*/
	public static final String[] jxHbaseUserFields = { "clientip", "response_time", "time_local", "method", "domain",
			"request", "protocol", "http_status", "body_bytes_sent", "referer", "user_agent", "hit_status" };
	public static final String[] jxHbaseOtherFields = { "firm_name", "http_range", "upstream_response_time",
			"time_unix", "hit_status_detail", "upstream_addr", "server_addr", "X_Info_Fetcher", "X_Info_ObjSize",
			"X_Info_request_id", "X_Info_MD5", "prov", "country", "city", "ISP", "latitude", "longitude", "blockid",
			"referer_host", "search_engine", "hackerid", "layer", "def1", "hit_status_whole", "host",
			"p_response_length", "p_server", "p_status", "request_from", "ruleid", "xff" };
	
	public static final String[] fusionHbaseOtherFields={"firm_name","http_range","upstream_response_time","time_unix","hit_status_detail","upstream_addr","server_addr",
		"X_Info_Fetcher","X_Info_ObjSize","X_Info_request_id","X_Info_MD5","prov","country","city","ISP","latitude","longitude","blockid","referer_host","search_engine",
		"hackerid","layer","def1","file_name","line_num"};
	
	public static final String[] bandNeedFields={"domain","http_status", "body_bytes_sent","hit_status","firm_name","time_unix", "prov", "country", "city", "ISP",  "layer", "def1","file_name"};

	public static void main(String[] args) {
//		String bodystr = "21312321@##@453/u0001543";// /
//		String[] values = bodystr.split("@##@");
//		for (int i = 0; i < values.length; i++) {
//			System.out.println("values[" + i + "]:" + values[i]);
//		}
		
		String str="\"abc\"";
		System.out.println(str);
	}
	/**
	 * 
	 * @param
	 * @param event
	 * @throws IOException
	 */
	public static Map<String, String> splitRankData2Map(byte[] bytes) {
		Map<String, String> hm = new HashMap<String, String>();
		if (bytes == null) {
			return hm;
		}
		String bodystr = new String(bytes);
		String[] values = bodystr.split(SEPARATOR);
		int len = FIELDS_Common.length;

		if (len > values.length) {
			len = values.length;
		}
		for (int i = 0; i < len; i++) {
			hm.put(FIELDS_Common[i], values[i]);
		}
		return hm;

	}
	
	/**
	 * 
	 * @param
	 * @param event
	 * @throws IOException
	 */
	public static Map<String, String> splitFusion2MapForBand(byte[] bytes) {
		Map<String, String> hm = new HashMap<String, String>();
		if (bytes == null) {
			return hm;
		}
		String bodystr = new String(bytes);
		String[] values = bodystr.split(SEPARATOR);
		int len = FusionBandFIELDS.length;

		if (len > values.length) {
			len = values.length;
		}
		for (int i = 0; i < len; i++) {
			hm.put(FusionBandFIELDS[i], values[i]);
		}
//		String request=hm.get("request");
//		if(request!=null){
//			request=string2Json(request);
//			hm.put("request", request);
//		}
//		String domain=hm.get("domain");
//		if(domain!=null){
//			domain=string2Json(domain);
//			hm.put("domain", domain);
//		}
		return hm;

	}
	/**
	 * 
	 * @param
	 * @param event
	 * @throws IOException
	 */
	public static Map<String, String> splitFusion2MapForDetail(byte[] bytes) {
		Map<String, String> hm = new HashMap<String, String>();
		if (bytes == null) {
			return hm;
		}
		String bodystr = new String(bytes);
		String[] values = bodystr.split(SEPARATOR);
		int len = FusionDetailFIELDS.length;

		if (len > values.length) {
			len = values.length;
		}
		for (int i = 0; i < len; i++) {
			hm.put(FusionDetailFIELDS[i], values[i]);
		}
//		String request=hm.get("request");
//		if(request!=null){
//			request=string2Json(request);
//			hm.put("request", request);
//		}
//		String domain=hm.get("domain");
//		if(domain!=null){
//			domain=string2Json(domain);
//			hm.put("domain", domain);
//		}
		return hm;

	}
	
//	static String string2Json(String s) {     
//	    StringBuffer sb = new StringBuffer ();     
//	    for (int i=0; i<s.length(); i++) { 
//	        char c = s.charAt(i);     
//	        switch (c) {     
//	        case '\"':     
//	            sb.append("\\\"");     
//	            break;     
//	        case '\\':     
//	            sb.append("\\\\");     
//	            break;     
//	        case '/':     
//	            sb.append("\\/");     
//	            break;     
//	        case '\b':     
//	            sb.append("\\b");     
//	            break;     
//	        case '\f':     
//	            sb.append("\\f");     
//	            break;     
//	        case '\n':     
//	            sb.append("\\n");     
//	            break;     
//	        case '\r':     
//	            sb.append("\\r");     
//	            break;     
//	        case '\t':     
//	            sb.append("\\t");     
//	            break;     
//	        default:     
//	            sb.append(c);     
//	        }
//	    }
//	    return sb.toString();     
//	 }  
	
	public static Map<String,String> setID(Map<String, String> hmValue,String prefix){
		StringBuffer sb = new StringBuffer(prefix);
		for (String key : FieldConst.keyStrs_xm_stat_fusion) {
			Object obj=hmValue.get(key);
			String str=null;
			if(obj==null){
				str="-";
			}else{
				str=obj.toString();
			}
			sb.append(str);
		}
		String md5Key = DigestUtils.md5Hex(sb.toString());
		hmValue.put("id", md5Key);
		// logger.debug("MD5:" + md5Key);
		return hmValue;
		
	}

	public static Map<String, String> splitJXHbase2Map(String userlog, String otherLog) {
		
		String[] userLogArray = userlog.split(HBASE_SEPATOR);
		String[] otherLogArray = otherLog.split(HBASE_SEPATOR);		
		
		Map<String, String> hmValue = new HashMap<String, String>();
		for (int i = 0; i < userLogArray.length; i++) {
			hmValue.put(jxHbaseUserFields[i], userLogArray[i]);			
		}
		for (int i = 0; i < otherLogArray.length; i++) {
			hmValue.put(jxHbaseOtherFields[i], otherLogArray[i]);
			
		}
		//将首尾的引号去掉
		String value1=hmValue.get(jxHbaseUserFields[0]);
		value1=dropMarks(value1);
		hmValue.put(jxHbaseUserFields[0], value1);
		
		String value2=hmValue.get(jxHbaseUserFields[jxHbaseUserFields.length-1]);
		value2=dropMarks(value2);
		hmValue.put(jxHbaseUserFields[jxHbaseUserFields.length-1], value2);
		
		String value3=hmValue.get(jxHbaseOtherFields[0]);
		value3=dropMarks(value3);
		hmValue.put(jxHbaseOtherFields[0], value3);
		
		String value4=hmValue.get(jxHbaseOtherFields[jxHbaseOtherFields.length-1]);
		value1=dropMarks(value4);
		hmValue.put(jxHbaseOtherFields[jxHbaseOtherFields.length-1], value4);
		
		return hmValue;
				 
	}
	

public static Map<String, String> splitFusionHbase2Map(String userlog, String otherLog) {
		
		String[] userLogArray = userlog.split(HBASE_SEPATOR);
		String[] otherLogArray = otherLog.split(HBASE_SEPATOR);		
		
		Map<String, String> hmValue = new HashMap<String, String>();
		for (int i = 0; i < userLogArray.length; i++) {
			hmValue.put(jxHbaseUserFields[i], userLogArray[i]);			
		}
		for (int i = 0; i < otherLogArray.length; i++) {
			hmValue.put(fusionHbaseOtherFields[i], otherLogArray[i]);
			
		}
		//将首尾的引号去掉
		String value1=hmValue.get(jxHbaseUserFields[0]);
		value1=dropMarks(value1);
		hmValue.put(jxHbaseUserFields[0], value1);
		
		String value2=hmValue.get(jxHbaseUserFields[jxHbaseUserFields.length-1]);
		value2=dropMarks(value2);
		hmValue.put(jxHbaseUserFields[jxHbaseUserFields.length-1], value2);
		
		String value3=hmValue.get(fusionHbaseOtherFields[0]);
		value3=dropMarks(value3);
		hmValue.put(fusionHbaseOtherFields[0], value3);
		
		String value4=hmValue.get(fusionHbaseOtherFields[fusionHbaseOtherFields.length-1]);
		value1=dropMarks(value4);
		hmValue.put(fusionHbaseOtherFields[fusionHbaseOtherFields.length-1], value4);
		
		Map<String, String> hmRes =convertToBandMap(hmValue);
		return hmRes;
				 
	}

    private static Map<String, String> convertToBandMap(Map<String, String> hmValue){
    	Map<String, String> hmBand=new HashMap<String, String>();
    	for(String key:bandNeedFields){
    		String value=hmValue.get(key);
    		hmBand.put(key, value);
    	}
    	return hmBand;
    	
    }
	
	public static String dropMarks(String str){
		if(str==null||str.length()<=0){
			return str;
		}
		int begin=0;
		int end=str.length();
		if(str.startsWith("\"")){
			begin=1;
		}
		if(str.endsWith("\"")){
			end=end-1;
		}
		str=str.substring(begin, end);
		return str;
	}

	/**
	 * 
	 * @param
	 * @param event
	 * @throws IOException
	 */
	public static Map<String, String> splitJX2MapForDetail(byte[] bytes) {
		Map<String, String> hm = new HashMap<String, String>();
		if (bytes == null) {
			return hm;
		}
		String bodystr = new String(bytes);
		String[] values = bodystr.split(SEPARATOR);
		int len = jxDetailFiledsArray.length;

		if (len > values.length) {
			len = values.length;
		}
		for (int i = 0; i < len; i++) {
			if (values[i] != null && values[i].length() > 0) {
				hm.put(jxDetailFiledsArray[i], values[i]);
			}
		}
		return hm;

	}
	
	/**
	 * 
	 * @param
	 * @param event
	 * @throws IOException
	 */
	public static Map<String, String> splitJX2MapForBand(byte[] bytes) {
		Map<String, String> hm = new HashMap<String, String>();
		if (bytes == null) {
			return hm;
		}
		String bodystr = new String(bytes);
		String[] values = bodystr.split(SEPARATOR);
		int len = jxBandFiledsArray.length;

		if (len > values.length) {
			len = values.length;
		}
		for (int i = 0; i < len; i++) {
			if (values[i] != null && values[i].length() > 0) {
				hm.put(jxBandFiledsArray[i], values[i]);
			}
		}
		return hm;

	}

}
