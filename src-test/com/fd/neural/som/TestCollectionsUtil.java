/**
 * 
 */
package com.fd.neural.som;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

/**
 * @author shawnkuo
 *
 */
public class TestCollectionsUtil {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<Integer> a = new ArrayList<Integer> ();
		a.add(1);
		a.add(2);
		a.add(3);
		a.add(4);
		a.add(5);
		
		List<Integer> b = new ArrayList<Integer> ();
		b.add(1);
		b.add(2);
		b.add(6);
		b.add(4);
		b.add(5);
		b.add(7);
		b.add(8);
		b.add(9);
	
		Collection union = CollectionUtils.intersection(a, b);
		System.out.println(union.size());
	}

}
