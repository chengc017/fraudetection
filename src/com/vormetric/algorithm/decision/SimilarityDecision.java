/**
 * 
 */
package com.vormetric.algorithm.decision;

/**
 * @author xioguo
 *
 */
public interface SimilarityDecision <T1, T2> {
	public Match match(T1 x, T2 y);
}
