/**
 * 
 */
package com.vormetric.fd.preprocess;

import java.io.File;

import org.encog.Encog;
import org.encog.app.analyst.AnalystFileFormat;
import org.encog.app.analyst.EncogAnalyst;
import org.encog.app.analyst.csv.normalize.AnalystNormalizeCSV;
import org.encog.app.analyst.wizard.AnalystWizard;
import org.encog.util.csv.CSVFormat;

/**
 * @author shawnkuo
 *
 */
public class NeuralCSVNormalizer {

	private File sourceFile = null;
	private File targetFile = null;

	public NeuralCSVNormalizer(String input, String output) {
		sourceFile = new File(input);
		targetFile = new File(output);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("NormalizeFile [input file] [target file]");
		} else {
			NeuralCSVNormalizer normalizer = new NeuralCSVNormalizer(args[0], args[1]);
			normalizer.normalize();
		}
	}

	public void normalize() {
		EncogAnalyst analyst = new EncogAnalyst();
		AnalystWizard wizard = new AnalystWizard(analyst);
		wizard.wizard(sourceFile, false, AnalystFileFormat.DECPNT_COMMA);

		final AnalystNormalizeCSV norm = new AnalystNormalizeCSV();
		norm.analyze(sourceFile, false, CSVFormat.ENGLISH, analyst);
		norm.setProduceOutputHeaders(false);
		norm.normalize(targetFile);
		Encog.getInstance().shutdown();
	}

}
