/**
 * 
 */
package com.vormetric.device.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author xioguo
 *
 */
public class Md5Utils {

	public static final int MD5_LENGTH = 16;
	
	public static byte [] md5sum(String s) {
		MessageDigest d;
		try {
			d = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 algorithm not avilable!", e);
		}
		return d.digest(Bytes.toBytes(s));
	}
}
