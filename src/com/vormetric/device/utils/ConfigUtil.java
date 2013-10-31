/**
 * 
 */
package com.vormetric.device.utils;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * @author xioguo
 *
 */
public class ConfigUtil {

	public final static String HBASE_MASTER = "hbase.zookeeper.quorum";
	
	private final static CompositeConfiguration config = new CompositeConfiguration();
	
	static{
		try {
			config.addConfiguration(new PropertiesConfiguration("conf/hbase.config"));
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}

	public static String getProperty(String str) {
		return config.getString(str);
	}
	
	public static String getHbaseHost() {
		return config.getString(HBASE_MASTER);
	}
}
