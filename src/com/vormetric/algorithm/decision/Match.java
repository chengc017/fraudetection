/**
 * 
 */
package com.vormetric.algorithm.decision;

/**
 * @author xioguo
 *
 */
public class Match {

	public double total;
	public double browser;
	public double plugin;
	public double os;
	public double connection;
	public boolean result = false;
	
	public Match(boolean match) {
		this(match, 0.0, 0.0, 0.0, 0.0, 0.0);
	}
	
	public Match(boolean result, double total, double browser, double plugin,
			double os, double connection) {
		this.result = result;
		this.total = total;
		this.browser = browser;
		this.plugin = plugin;
		this.os = os;
		this.connection = connection;
	}
}
