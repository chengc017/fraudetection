/**
 * 
 */
package com.vormetric.algorithm.decision;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;

import org.apache.hadoop.io.Text;

import com.vormetric.algorithm.similarities.Similarity;
import com.vormetric.device.model.DeviceModel;

/**
 * @author shawnkuo
 *
 */
public class SVMDeviceSimilarityDecision {

	private Similarity similarity;
	private static svm_model model;
	
	public SVMDeviceSimilarityDecision (Similarity similarity) {
		this.similarity = similarity;
	}
	
	//load svm model
	static {
		try {
			model = svm.svm_load_model("train/ds.model");  //device similarity model
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean match(Object a, Object b) {
		if(a instanceof DeviceModel && b instanceof DeviceModel) {
			return match((DeviceModel)a, (DeviceModel)b);
		} else if(a instanceof DeviceModel && b instanceof List<?>) {
			return match((DeviceModel)a, (ArrayList<DeviceModel>)b);
		} else if(a instanceof List<?> && b instanceof DeviceModel) {
			return match((DeviceModel)b, (ArrayList<DeviceModel>)a);
		} else if (a instanceof List<?> && b instanceof List<?>) {
			return match((ArrayList<DeviceModel>)a, (ArrayList<DeviceModel>)b);
		}
		
		return false;
	}
	
	protected boolean match(ArrayList<DeviceModel> a, ArrayList<DeviceModel> b) {
		boolean match = false;
		for(DeviceModel dm :a) {
			match=match(dm, b);
			if(match) break;
		}
		return match;
	}
	
	protected boolean match(DeviceModel a, ArrayList<DeviceModel> b) {
		boolean match = false;
		for(DeviceModel dm:b) {
			if(match(a,dm)) {
				match = true;
				break;
			}
		}
		return match;
	}
	
	private boolean match(DeviceModel dmX, DeviceModel dmY) {
		double score = similarity.similarity(dmX.all(), dmY.all());
		double browser = similarity.similarity(
				convert(dmX.getBrowserAttributes()),
				convert(dmY.getBrowserAttributes()));
		double plugin = similarity.similarity(
				convert(dmX.getPluginAttributes()),
				convert(dmY.getPluginAttributes()));
		double os = similarity.similarity(
				convert(dmX.getOsAttributes()),
				convert(dmY.getOsAttributes()));
		double connection = similarity.similarity(
				convert(dmX.getConnectionAttributes()),
				convert(dmY.getConnectionAttributes()));
		boolean result = match(browser, plugin, os, connection);
//		if(result)
//			System.out.println("----------## score: " + score + " --> " + result);
		return result;
	}
	
	public boolean match(double browser, double plugin, double os, double connection) {
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
	
	private List<String> convert(List<Text> list) {
		List<String> strList = new LinkedList<String> ();
		for(Text item:list) {
			strList.add(item.toString());
		}
		return strList;
	}
}
