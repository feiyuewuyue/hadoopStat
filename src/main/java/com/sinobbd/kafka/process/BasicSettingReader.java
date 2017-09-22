package com.sinobbd.kafka.process;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

public class BasicSettingReader {
private static final Logger LOG = Logger.getLogger(BasicSettingReader.class);
	
	private static final String CONFIG_FILE = "basicSetting.yaml";
	public static Map<Object, Object> configration;//存储配置文件topology.xml和connection信息	
	/**
	 * 初始化
	 */
	static{
		loadYaml(CONFIG_FILE);		
		LOG.info("PropertyReader初始化成功");
	}
	
	private static void loadYaml(String confPath) {
		Yaml yaml = new Yaml();
		InputStream stream = null;
		try {
			stream = BasicSettingReader.class.getClassLoader().getResourceAsStream(confPath);
			configration = (Map) yaml.load(stream);
			if (configration == null || configration.isEmpty()) {
				throw new RuntimeException("Failed to read config file");
			}
		} catch (Exception e1) {
			LOG.error(e1.getMessage(),e1);
			throw new RuntimeException("Failed to read config file: " + confPath);
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException e) {
					LOG.error(e.getMessage(),e);
				}
			}
		}
	}


	
}