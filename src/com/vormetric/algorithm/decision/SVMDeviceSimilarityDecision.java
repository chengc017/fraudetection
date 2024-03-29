/**
 * 
 */
package com.vormetric.algorithm.decision;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;

import com.vormetric.algorithm.similarities.Similarity;
import com.vormetric.device.model.DeviceModel;

/**
 * @author shawnkuo
 *
 */
public class SVMDeviceSimilarityDecision<T1, T2> implements SimilarityDecision<T1, T2> {

	private Similarity similarity;
	private static svm_model model;
	
	public SVMDeviceSimilarityDecision (Similarity similarity) {
		this.similarity = similarity;
		try {
			model = svm.svm_load_model(new BufferedReader(
					new InputStreamReader(this.getClass()
							.getResourceAsStream("ds.model"))));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//load svm model
	static {
//		try {
			//model = svm.svm_load_model("train/ds.model");  //device similarity model
//			model = svm.svm_load_model(new BufferedReader(
//					new InputStreamReader(SVMDeviceSimilarityDecision.class
//							.getResourceAsStream("train/ds.model"))));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
	
	
	public Match match(Object a, Object b) {
		if(a instanceof DeviceModel && b instanceof DeviceModel) {
			return match((DeviceModel)a, (DeviceModel)b);
		} else if(a instanceof DeviceModel && b instanceof List<?>) {
			return match((DeviceModel)a, (ArrayList<DeviceModel>)b);
		} else if(a instanceof List<?> && b instanceof DeviceModel) {
			return match((DeviceModel)b, (ArrayList<DeviceModel>)a);
		} else if (a instanceof List<?> && b instanceof List<?>) {
			return match((ArrayList<DeviceModel>)a, (ArrayList<DeviceModel>)b);
		} else {
			return new Match(false);
		}
	}
	
	protected Match match(ArrayList<DeviceModel> a, ArrayList<DeviceModel> b) {
		Match match = new Match(false);
		for(DeviceModel dm :a) {
			match=match(dm, b);
			if(match.result) break;
		}
		return match;
	}
	
	protected Match match(DeviceModel a, ArrayList<DeviceModel> b) {
		Match match = new Match(false);
		for(DeviceModel dm : b) {
			match = match(a, dm);
			if(match.result) {
				break;
			}
		}
		return match;
	}
	
	public Match match(DeviceModel dmX, DeviceModel dmY) {
		double total = similarity.similarity(dmX.all(), dmY.all());
		double browser = similarity.similarity(
				dmX.getBrowserAttributes(),
				dmY.getBrowserAttributes());
		double plugin = similarity.similarity(
				dmX.getPluginAttributes(),
				dmY.getPluginAttributes());
		double os = similarity.similarity(
				dmX.getOsAttributes(),
				dmY.getOsAttributes());
		double connection = similarity.similarity(
				dmX.getConnectionAttributes(),
				dmY.getConnectionAttributes());
		boolean match = match(browser, plugin, os, connection);
		Match result = new Match(match, total, browser, plugin, os,
				connection);
		return result;
	}
	
	protected boolean match(double browser, double plugin, double os, double connection) {
		svm_node [] nodes = new svm_node [4];
		nodes[0] = new svm_node();
		nodes[0].index = 1;
		nodes[0].value = browser;
		
		nodes[1] = new svm_node();
		nodes[1].index = 2;
		nodes[1].value = plugin;
		
		nodes[2] = new svm_node();
		nodes[2].index = 3;
		nodes[2].value = os;
		
		nodes[3] = new svm_node();
		nodes[3].index = 4;
		nodes[3].value = connection;
		
		double value = svm.svm_predict(model, nodes);
		return ((int)value) == 1 ? true : false;
	}
}
