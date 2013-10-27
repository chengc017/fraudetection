/**
 * 
 */
package com.vormetric.algorithm.similarities;

import java.util.List;

/**
 * @author xioguo
 *
 */
public interface Similarity {

	public double similarity(String[] x, String[] y);
	public double similarity(List<String> x, List<String> y) ;
}
