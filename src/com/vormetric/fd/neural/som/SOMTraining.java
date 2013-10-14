/**
 * 
 */
package com.vormetric.fd.neural.som;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.encog.Encog;
import org.encog.mathutil.rbf.GaussianFunction;
import org.encog.mathutil.rbf.RadialBasisFunction;
import org.encog.ml.data.MLDataPair;
import org.encog.ml.data.MLDataSet;
import org.encog.neural.som.SOM;
import org.encog.neural.som.training.basic.BasicTrainSOM;
import org.encog.neural.som.training.basic.neighborhood.NeighborhoodRBF1D;
import org.encog.persist.EncogDirectoryPersistence;
import org.encog.util.csv.CSVFormat;
import org.encog.util.simple.TrainingSetUtil;

/**
 * @author shawnkuo
 *
 */
public class SOMTraining {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage:\nTXCSV [normalized.csv] \tpath to save trained model [txsom.eg]");
		} else {
			//load csv data
			final MLDataSet training = TrainingSetUtil.loadCSVTOMemory(
				CSVFormat.ENGLISH, args[0], false, 44, 0);
			
			//create network
			SOM network = new SOM(44,3);
			network.reset();
			
			RadialBasisFunction radial = new GaussianFunction(0.0,1.0,1.0);
			NeighborhoodRBF1D bubble = new NeighborhoodRBF1D(radial);
			BasicTrainSOM train = new BasicTrainSOM(
					network,
					0.3,
					training,
					bubble);
			train.setForceWinner(true);
			
			int iteration = 0;
			for(iteration = 0;iteration<=2000;iteration++)
			{
				train.iteration();
			}
			
			System.out.println("Saving model");
			EncogDirectoryPersistence.saveObject(new File(args[1]), network);
			
			print(training, network);
			
			Encog.getInstance().shutdown();
		}
	}
	
	public static void print(MLDataSet training, SOM network) {
		Iterator<MLDataPair> it = training.iterator();
		
		Map<Integer, List<Integer>> clusters = new HashMap<Integer, List<Integer>> (); 
		int i = 0;
		while(it.hasNext()) {
			MLDataPair dp = it.next();
			int cluster = network.classify(dp.getInput());
			if(clusters.containsKey(cluster)) {
				clusters.get(cluster).add(i);
			} else {
				List<Integer> indexList = new ArrayList<Integer>();
				indexList.add(i);
				clusters.put(cluster, indexList);
			}
			i++;
		}
		
		for(Entry<Integer, List<Integer>> entry : clusters.entrySet()) {
			System.out.println("Group "+ entry.getKey() + " - "+ entry.getValue().size() +": ");
			List<Integer> indexlst = entry.getValue();
			for(int index:indexlst) {
				System.out.print(index+"\t");
			}
			System.out.println("\n");
		}
	}

}
