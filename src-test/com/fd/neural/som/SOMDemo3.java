/**
 * 
 */
package com.fd.neural.som;

import java.util.ArrayList;
import java.util.List;

import org.encog.Encog;
import org.encog.mathutil.matrices.Matrix;
import org.encog.mathutil.rbf.GaussianFunction;
import org.encog.mathutil.rbf.RadialBasisFunction;
import org.encog.ml.data.MLData;
import org.encog.ml.data.MLDataSet;
import org.encog.ml.data.basic.BasicMLData;
import org.encog.ml.data.basic.BasicMLDataSet;
import org.encog.neural.som.SOM;
import org.encog.neural.som.training.basic.BasicTrainSOM;
import org.encog.neural.som.training.basic.neighborhood.NeighborhoodRBF1D;
import org.encog.util.arrayutil.NormalizationAction;
import org.encog.util.arrayutil.NormalizedField;


/**
 * @author shawnkuo
 *
 */
public class SOMDemo3 {
	public static NormalizedField norm = new NormalizedField(NormalizationAction.Normalize, 
			null,10,0,1,-1);
	
	public static double XOR_INPUT[][] = {
		{5.1, 3.5, 1.4, 0.2},
		{4.9, 3.0, 1.4, 0.2},
		{4.7, 3.2, 1.3, 0.2},
		{7.0, 3.2, 4.7, 1.4},
		{6.4, 3.2, 4.5, 1.5},
		{6.9, 3.1, 4.9, 1.5},
		{6.3, 3.3, 6.0, 2.5},
		{5.8, 2.7, 5.1, 1.9},
		{7.1, 3.0, 5.9, 2.1}
	};

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// create data set
		MLDataSet training = new BasicMLDataSet();
		List<MLData> dataSet = prepareSamples();
		for(MLData data:dataSet) {
			training.add(data);
		}
		
		// create network
		SOM network = new SOM(4,3);
		network.reset();
		
		RadialBasisFunction radial = new GaussianFunction(0.0,1.0,1.0);
		NeighborhoodRBF1D bubble = new NeighborhoodRBF1D(radial);
		BasicTrainSOM train = new BasicTrainSOM(
				network,
				0.4,
				training,
				bubble);
		train.setForceWinner(true);
		
		int iteration = 0;
		for(iteration = 0;iteration<=1000;iteration++)
		{
			train.iteration();
			System.out.println("Iteration: " + iteration + ", Error:" + train.getError());
		}
		
		for(MLData data : dataSet) {
			System.out.println(network.classify(data));
		}
		Encog.getInstance().shutdown();
	}

	private static List<MLData> prepareSamples() {
		List<MLData> samples = new ArrayList<MLData> ();
		
		MLData data1 = new BasicMLData(4);
		data1.setData(0, normalize(5.1));
		data1.setData(1, normalize(3.5));
		data1.setData(2, normalize(1.4));
		data1.setData(3, normalize(0.2));
		samples.add(data1);
		
		MLData data9 = new BasicMLData(4);
		data9.setData(0, normalize(7.1));
		data9.setData(1, normalize(3.0));
		data9.setData(2, normalize(5.9));
		data9.setData(3, normalize(2.1));
		samples.add(data9);
		
		MLData data2 = new BasicMLData(4);
		data2.setData(0, normalize(4.9));
		data2.setData(1, normalize(3.0));
		data2.setData(2, normalize(1.4));
		data2.setData(3, normalize(0.2));
		samples.add(data2);
		
		MLData data4 = new BasicMLData(4);
		data4.setData(0, normalize(7.0));
		data4.setData(1, normalize(3.2));
		data4.setData(2, normalize(4.7));
		data4.setData(3, normalize(1.4));
		samples.add(data4);
		
		MLData data7 = new BasicMLData(4);
		data7.setData(0, normalize(6.3));
		data7.setData(1, normalize(3.3));
		data7.setData(2, normalize(6.0));
		data7.setData(3, normalize(2.5));
		samples.add(data7);
		
		MLData data5 = new BasicMLData(4);
		data5.setData(0, normalize(6.4));
		data5.setData(1, normalize(3.2));
		data5.setData(2, normalize(4.5));
		data5.setData(3, normalize(1.5));
		samples.add(data5);
		
		MLData data3 = new BasicMLData(4);
		data3.setData(0, normalize(4.7));
		data3.setData(1, normalize(3.2));
		data3.setData(2, normalize(1.3));
		data3.setData(3, normalize(0.2));
		samples.add(data3);
		
		MLData data6 = new BasicMLData(4);
		data6.setData(0, normalize(6.9));
		data6.setData(1, normalize(3.1));
		data6.setData(2, normalize(4.9));
		data6.setData(3, normalize(1.5));
		samples.add(data6);
		
		MLData data8 = new BasicMLData(4);
		data8.setData(0, normalize(5.8));
		data8.setData(1, normalize(2.7));
		data8.setData(2, normalize(5.1));
		data8.setData(3, normalize(1.9));
		samples.add(data8);
		
		return samples;
	}
	
	public static double normalize(double x) {
		return norm.normalize(x);
	}
}
