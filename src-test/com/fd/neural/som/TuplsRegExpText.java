/**
 * 
 */
package com.fd.neural.som;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * @author shawnkuo
 *
 */
public class TuplsRegExpText {

	private static final Pattern REGEX =
        Pattern.compile("\\[(.*?)\\]");
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String test = "[a],[0],[223x540],[\"+sa:232:s:f,d\"],[(ss,s)],[],[]";
		new TuplsRegExpText().canonicalize(test);
	}

	public String canonicalize(String str) {
		return StringUtils.isNotEmpty(str) ? extract(str, REGEX
				.matcher(str)) : "";
	}
	
	public String extract(String str, Matcher matcher) {
		List<String> strList = new ArrayList<String> ();
		while(matcher.find()) {
			System.out.println(matcher.group(1));
			strList.add(matcher.group(1));
		}
		System.out.println(strList.size());
		return null;
	}
	
}
