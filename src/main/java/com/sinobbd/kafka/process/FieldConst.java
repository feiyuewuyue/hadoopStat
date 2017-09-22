package com.sinobbd.kafka.process;

public interface FieldConst {
	public static final String[] keyStrs_xm_stat_jx = new String[] { "layer", "firm_name", "domain", "prov", "ISP",
			"hit_status", "http_status" };
	
	public static final String[] keyStrs_xm_stat_fusion = new String[] { "file_name","layer", "firm_name", "domain", "prov", "ISP",
		"hit_status", "http_status","def1" };
	
	public static final String[] keyStrs_xm_jx_host = new String[] { "host", "http_status","domain"};
	

	public static final String field_id="id";
//	public static final String[] keyStrs_1m_band_stat = new String[] { "layer", "firm_name", "domain", "prov", "ISP" };
	public static final String field_time_unix = "time_unix";// 校验非空
	public static final String field_time_mt = "time_mt";
	public static final String field_size = "body_bytes_sent";
	public static final String field_http_status = "http_status";
	public static final String field_p_response_time = "p_response_time";
	public static final String field_response_time = "response_time";
	public static final String field_p_response_length = "p_response_length";
	public static final String field_hit_status = "hit_status";
	public static final String field_continent_code = "continent_code";
	public static final String field_continent_name = "continent_name";
	public static final String field_file_name ="file_name";
	public static final String field_line_num ="line_num";
	public static final String field_X_Info_request_id ="X_Info_request_id";
	public static final String field_layer="layer";
	public static final String field_request="request";
	
	public static final String field_firm_name = "firm_name";
	public static final String field_domain = "domain";
	public static final String field_clientip = "clientip";
	public static final String field_es_size = "traffic";
    public static final String field_es_size_pm="traffic_pm";
	public static final String field_es_req_count = "req_count";

	public static final String field_es_fromtime = "from_time";
	public static final String field_es_time_local = "time_local";
	public static final String field_es_timestamp = "@timestamp";
	
	public static final String field_es_logdate = "log_date";
	
	public static final String count_value_cn="中国";
	
	public static final String index_log_error="error-log";
	public static final String index_log_pre="nginx-test-0t";
	public static final String index_log_type="fluentd";
	public static final String index_stat_1m_pre="statsdata_mx_";
	public static final String index_stat_5m_pre="statsdata_5mx_";
	public static final String index_stat_1h_pre="statsdata_hx_";
//	public static final String index_stat_1m_formal="statsdata_m_";
//	public static final String index_stat_5m_formal="statsdata_5m_";
//	public static final String index_stat_1h_formal="statsdata_h_";
	public static final String index_stat_type="bandwidth";
	
	public static final int ES_COMMIT_NUM=200000;
	//数据统计类型
	//一分钟的带宽统计
	public static final String prefix_agg_1m = "m1";
	//5分钟维度的带宽统计
	public static final String prefix_agg_5m = "m5";
	//JX数据按照host+status统计数据
	public static final String prefix_agg_hoststatus="hst_sts";
	
	
	public static final String field_es_kafkaoffset = "offset";
			
	

}
