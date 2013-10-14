/**
 * 
 */
package com.vormetric.fd;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.encog.Encog;
import org.encog.ml.data.MLDataPair;
import org.encog.ml.data.MLDataSet;
import org.encog.neural.som.SOM;
import org.encog.persist.EncogDirectoryPersistence;
import org.encog.util.csv.CSVFormat;
import org.encog.util.simple.TrainingSetUtil;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.vormetric.fd.preprocess.NeuralCSVNormalizer;

/**
 * @author shawnkuo
 *
 */
public class InMemoryEvalClient {

	public static final Log logger = LogFactory.getLog(InMemoryEvalClient.class);

	public static final String TMP_CSV_FILE = "./selected.csv";

	public static final String TMP_NORM_FILE = "./normalized.csv";

	public String inputCsv = null;
	public String modelFile = null;

	public InMemoryEvalClient(String inputCsv, String modelFile) {
		this.inputCsv = inputCsv;
		this.modelFile = modelFile;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length < 2) {
			System.out.println("InMemoryEvalClient [input csv file path] [model file path]");
		} else {
			InMemoryEvalClient client = new InMemoryEvalClient(args[0], args[1]);
			//preprocess the data
			logger.info("preprocess input data...");
			client.preprocess();
			//load previous trained model
			logger.info("load model and evaluate...");
			client.loadAndEvaluate();
		}
	}

	private void preprocess() {
		//select records
		logger.info("extracting data...");
		List<String[]> records = extract(inputCsv);
		//output selected records
		logger.info("saving extracted data.");
		write(records);
		//normalize data
		logger.info("normalizing data...");
		normalize(TMP_CSV_FILE);
	}
	
	private void loadAndEvaluate() {
		//load dataset
		logger.info("loading dataset...");
		final MLDataSet training = TrainingSetUtil.loadCSVTOMemory(
				CSVFormat.ENGLISH, TMP_NORM_FILE, false, 44, 0);

		//build network
		logger.info("bulding neural network...");
		SOM network = (SOM) EncogDirectoryPersistence
				.loadObject(new File(modelFile));

		//cluster
		logger.info("run neural network...");
		Iterator<MLDataPair> it = training.iterator();
		Map<Integer, List<Integer>> somCluster = new HashMap<Integer, List<Integer>> (); 
		int i = 0;
		while(it.hasNext()) {
			MLDataPair dp = it.next();
			int cluster = network.classify(dp.getInput());
			if(somCluster.containsKey(cluster)) {
				somCluster.get(cluster).add(i);
			} else {
				List<Integer> lst = new ArrayList<Integer>();
				lst.add(i);
				somCluster.put(cluster, lst);
			}
			i++;
		} 
		
		//evaluate result
		logger.info("clustering finished, ready to evaluate result.");
		logger.info("preparing supervised cluster.");
		Map<Integer, List<Integer>> supervisedCluster = supervising();
		logger.info("evaluating.");
		evaluate(somCluster, supervisedCluster);
		
		Encog.getInstance().shutdown();
	}
	
	private void evaluate(Map<Integer, List<Integer>> somCluster, Map<Integer, List<Integer>> supervisedCluster) {
		//1. find nearest list, pair together to compare
		//TODO PROBLEM:
		// 15 25 60
		// 70(pass) 30(review) 10(reject)
		logger.info("finding nearest clusters.");
		for(Entry<Integer, List<Integer>> entrySet : somCluster.entrySet()) {
			List<Integer> array = entrySet.getValue();
			double nearest = 0;
			int nearestIndex = 0;
			for(Entry<Integer, List<Integer>> entrySet2 : supervisedCluster.entrySet()) {
				List<Integer> target = entrySet2.getValue();
				Collection union = CollectionUtils.intersection(array, target);
				double distance = nearest((double)union.size(), (double)array.size(), (double)target.size());
				if(distance > nearest) {
					nearest = distance;
					nearestIndex = entrySet2.getKey();
				}
			}
			//compair
			double truePositive = 0.0;
			double falsePostive = 0.0;
			truePositive = (double)nearest/(double)supervisedCluster.get(nearestIndex).size();
			falsePostive = (double)(array.size()-nearest)/(double)supervisedCluster.get(nearestIndex).size();
			if(nearestIndex ==0) {
				System.out.println("Pass Transaction: TRUE POSITIVE: " + truePositive + "\tFALSE POSITIVE: " +  falsePostive);
			} else if(nearestIndex ==1) {
				System.out.println("Review Transaction: TRUE POSITIVE: " + truePositive + "\tFALSE POSITIVE: " +  falsePostive);
			} else if(nearestIndex ==2) {
				System.out.println("Reject Transaction: TRUE POSITIVE: " + truePositive + "\tFALSE POSITIVE: " +  falsePostive);
			} 
			supervisedCluster.remove(nearestIndex);
		}		
	}
	
	private double nearest(double unionSize, double leftSize, double rightSize) {
		return 1-Math.log10(Math.abs(leftSize-rightSize)/leftSize)*unionSize;
	}
	 

	/**
	 * step1
	 * output extracted result into NN.csv file
	 * @input String the original CSV file
	 * @output String extracted file output path
	 * */
	private List<String[]> extract(String file) {
		CSVReader reader = null;
		List<String[]> extractedRecords = new ArrayList<String[]> ();
		try {
			reader = new CSVReader(new FileReader(file));
			String row[]  = null;
			while ((row = reader.readNext()) != null) {
				String txtype = row[4];
				if(txtype.equals("payment")) {
					List<String> valueList = new ArrayList<String> ();
					valueList.add(row[26].equals("") ? "-99" : row[26]); 		//device_match_result
					valueList.add(row[27].equals("") ? "-99" : row[27]);		//device_result
					valueList.add(row[30].equals("") ? "-99" : row[30]);		//enabled_ck
					valueList.add(row[31].equals("") ? "-99" : row[31]);		//enabled_fl
					valueList.add(row[32].equals("") ? "-99" : row[32]);		//enabled_im
					valueList.add(row[33].equals("") ? "-99" : row[33]);		//enabled_js
					valueList.add(row[50].equals("") ? "-99" : row[50]); 		//image_loaded
					valueList.add(row[65].equals("") ? "-99" : row[65]);		//screen_res	82.3%
					valueList.add(row[91].equals("") ? "-99" : row[91]);		//ua_browser	91%	
					valueList.add(row[92].equals("") ? "-99" : row[92]);		//ua_mobile		15%	
					valueList.add(row[93].equals("") ? "-99" : row[93]);		//ua_os		87%
					valueList.add(row[101].equals("") ? "-99" : row[101]);		//request_duration	100%
					valueList.add(row[109].equals("") ? "-99" : row[109]);		//mime_type_number	52%
					valueList.add(row[121].equals("") ? "-99" : row[121]);		//fuzzy_device_id_confidence
					valueList.add(row[123].equals("") ? "-99" : row[123]);		//fuzzy_device_match_result
					valueList.add(row[131].equals("") ? "-99" : row[131]);		//fuzzy_device_result
					valueList.add(row[163].equals("") ? "-99" : row[163]);		//true_ip_score
					valueList.add(row[164].equals("") ? "-99" : row[164]);		//true_ip_worst_score
					valueList.add(row[171].equals("") ? "-99" : row[171]);		//proxy_ip
					valueList.add(row[180].equals("") ? "-99" : row[180]);		//proxy_ip_result
					valueList.add(row[181].equals("") ? "-99" : row[181]);		//proxy_ip_score
					valueList.add(row[182].equals("") ? "-99" : row[182]);		//proxy_ip_worst_score
					valueList.add(row[183].equals("") ? "-99" : row[183]);		//proxy_type
					valueList.add(row[193].equals("") ? "-99" : row[193]);		//account_name_result
					valueList.add(row[194].equals("") ? "-99" : row[194]);		//account_name_score
					valueList.add(row[195].equals("") ? "-99" : row[195]);		//account_name_worst_score
					valueList.add(row[204].equals("") ? "-99" : row[204]);		//account_login_result
					valueList.add(row[215].equals("") ? "-99" : row[215]);		//password_hash_result
					valueList.add(row[226].equals("") ? "-99" : row[226]);		//account_number_result
					valueList.add(row[272].equals("") ? "-99" : row[272]);		//account_email_result
					valueList.add(row[284].equals("") ? "-99" : row[284]);		//account_address_result	16.1%
					valueList.add(row[339].equals("") ? "-99" : row[339]);		//transaction_amount	19.7%
					valueList.add(row[342].equals("") ? "-99" : row[342]);		//transaction_currency	18.5%
					valueList.add(row[387].equals("") ? "-99" : row[387]);		//tcp_os_sig_adv_mss	63.8%
					valueList.add(row[388].equals("") ? "-99" : row[388]);		//tcp_os_sig_snd_mss	63.4%
					valueList.add(row[389].equals("") ? "-99" : row[389]);		//tcp_os_sig_rcv_mss	63.4%
					valueList.add(row[390].equals("") ? "-99" : row[390]);		//http_os_sig_adv_mss	88.2%
					valueList.add(row[391].equals("") ? "-99" : row[391]);		//http_os_sig_snd_mss	88.2%
					valueList.add(row[392].equals("") ? "-99" : row[392]);		//http_os_sig_rcv_mss	88.2%
					valueList.add(row[393].equals("") ? "-99" : row[393]);		//tcp_os_sig_ttl	63.4%
					valueList.add(row[394].equals("") ? "-99" : row[394]);		//http_os_sig_ttl	88.2%
					valueList.add(row[395].equals("") ? "-99" : row[395]);		//profiling_delta	88.2%
					valueList.add(row[396].equals("") ? "-99" : row[396]);		//profiling_site_id		88.2%
					valueList.add(row[405].equals("") ? "-99" : row[405]);	
					String [] sb = new String[valueList.size()];
					valueList.toArray(sb);
					extractedRecords.add(sb);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		List<String[]> result = new ArrayList<String []>(extractedRecords.size());
		result.addAll(extractedRecords);
		return result;
	}

	private void write(List<String[]> records) {
		try {
			CSVWriter writer = new CSVWriter(new FileWriter(TMP_CSV_FILE));
			writer.writeAll(records);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * step2
	 * normalize data
	 * @input String the file to be normalized
	 * @output String the output file
	 * */
	private void normalize(String file) {
		NeuralCSVNormalizer normalizer = new NeuralCSVNormalizer(file, TMP_NORM_FILE);
		normalizer.normalize();
	}
	
	private Map<Integer, List<Integer>> supervising() {
		CSVReader reader = null;
		Map<Integer, List<Integer>> cluster = new HashMap<Integer,List<Integer>> ();
		List<Integer> passCluster = new ArrayList<Integer> ();
		cluster.put(0, passCluster);
		List<Integer> reviewCluster = new ArrayList<Integer> ();
		cluster.put(1, reviewCluster);
		List<Integer> rejectCluster = new ArrayList<Integer> ();
		cluster.put(2, rejectCluster);
		try {
			reader = new CSVReader(new FileReader(inputCsv));
			String row[]  = null;
			int index = 0;
			while ((row = reader.readNext()) != null) {
				String eventType = row[4];
				String reviewStatus = row[9];
				if(!eventType.equals("payment"))
					continue;
				if(reviewStatus.equals("pass")) {
					passCluster.add(index);
				} else if(reviewStatus.equals("review")) {
					reviewCluster.add(index);
				} else if(reviewStatus.equals("reject")) {
					rejectCluster.add(index);
				} 
				index++;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return cluster;
	}

}
