/**
 * 
 */
package com.vormetric.device.extract;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * @author xioguo
 *
 */
public class AttibutesConvertor {

	private static final Logger logger = Logger.getLogger(AttibutesConvertor.class);
	
	private static final Pattern REGEX =
	        Pattern.compile("\\[(.*?)\\]");
	
	private Matcher matcher;
	
	private String deviceString;
	
	private LinkedList<String> all;
	
	private List<String> emptyList = new ArrayList<String>();
	
	public static final int BROWSER_ATT_INDEX_START = 0;
	public static final int BROWSER_ATT_LENGTH = 13;
	public static final int BROWSER_ATT_INDEX_END = 13;
	
	public static final int PLUGIN_ATT_INDEX_START = 13;
	public static final int PLUGIN_ATT_LENGTH = 8;
	public static final int PLUGIN_ATT_INDEX_END = 21;
	
	public static final int OS_ATT_INDEX_START = 21;
	public static final int OS_ATT_LENGTH = 10;
	public static final int OS_ATT_INDEX_END = 31;
	
	public static final int CONNECTION_ATT_INDEX_START = 31;
	public static final int CONNECTION_ATT_LENGTH = 13;
	public static final int CONNECTION_ATT_INDEX_END = 44;
	
	public static final int TOTAL_ATT_LENGTH = BROWSER_ATT_LENGTH
			+ PLUGIN_ATT_LENGTH + OS_ATT_LENGTH + CONNECTION_ATT_LENGTH;
	
	private boolean valid = false;
	
	public AttibutesConvertor (String deviceString) {
		this.deviceString = deviceString;
		matcher = REGEX.matcher(deviceString);
		convert();
	}
	
	protected void convert() {
		all = StringUtils.isNotEmpty(deviceString) ? doConvert(deviceString,
				matcher) : new LinkedList<String>();
		if(all.size() != TOTAL_ATT_LENGTH) {
			logger.error("Invalid device string ### " + deviceString + " ###");
		} else {
			valid = true;
		}
	}
	
	public List<String> all() {
		return all;
	}
	
	public List<String> getBrowserAtts() {
		return valid ? all.subList(
				BROWSER_ATT_INDEX_START, BROWSER_ATT_INDEX_END) : emptyList;
	}
	
	public List<String> getPluginAtts() {
		return valid ? all.subList(
				PLUGIN_ATT_INDEX_START, PLUGIN_ATT_INDEX_END) : emptyList;
	}
	
	public List<String> getOSAtts() {
		return valid ? all.subList(
				OS_ATT_INDEX_START, OS_ATT_INDEX_END) : emptyList;
	}
	
	public List<String> getConnAtts() {
		return valid ? all.subList(
				CONNECTION_ATT_INDEX_START, CONNECTION_ATT_INDEX_END) : emptyList;
	}
	
	private static LinkedList<String> doConvert(String str, Matcher matcher) {
		LinkedList<String> attList = new LinkedList<String> ();
		while(matcher.find()) {
			attList.add(matcher.group(1));
		}
		return attList;
	}
	
	public String getString() {
		return deviceString;
	}
	
	public boolean isValid() {
		return valid;
	}
}
