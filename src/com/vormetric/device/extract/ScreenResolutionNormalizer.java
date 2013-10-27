/**
 * 
 */
package com.vormetric.device.extract;

import org.apache.commons.lang.StringUtils;

/**
 * @author xioguo
 *
 */
public class ScreenResolutionNormalizer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String screen = "480x800";
		String expected = "800x480";
		System.out.println(ScreenResolutionNormalizer.normalize(screen).equals(expected));
	}

	public static String normalize(String screen) {
		if(screen == null || screen.equals("")) {
			return "";
		}
		String [] sizes = screen.split("x");
		if(sizes.length == 2) {
			if (!StringUtils.isNumeric(sizes[0])
					|| !StringUtils.isNumeric(sizes[1]))
				return screen;
			int screenX = Integer.valueOf(sizes[0]);
			int screenY = Integer.valueOf(sizes[1]);
			if(screenX > screenY) {
				return String.valueOf(screenX)+"x"+String.valueOf(screenY);
			} else {
				return String.valueOf(screenY)+"x"+String.valueOf(screenX);
			}
		} else {
			return screen;
		}
	}
}
