/**
 * 
 */
package com.vormetric.device;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CSVLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CSVTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vormetric.algorithm.similarities.JaccardCoefficientSimilarity;
import com.vormetric.device.extract.DeviceAttributeExtractor;
import com.vormetric.device.model.DeviceModel;

/**
 * @author xioguo
 *
 */
public class DeviceSimilaritySVMTrainner extends Configured implements Tool {

	public static final Log logger = LogFactory.getLog(DeviceSimilaritySVMTrainner.class);
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("Need input and output path to process the job");
			return 0;
		}
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		Configuration conf = getConf();
		// Performance tuning
		//2. reusing the JVM
		conf.setInt("mapred.job.reuse.jvm.num.tasks", 3);
		
		conf.set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
		conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
		conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
		//
		conf.setBoolean("mapred.input.dir.recursive", true);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)) {
			fs.delete(out, true);
		}
		
		Job job = new Job(conf);
		job.setJobName("Device Similarity SVM Trainner");
		job.setJarByClass(DeviceSimilaritySVMTrainner.class);
		
		job.setInputFormatClass(CSVTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// map's output
		job.setOutputKeyClass(Text.class);  		
		job.setOutputValueClass(DeviceModel.class); 
		
		job.setMapperClass(DeviceSimilaritySVMMapper.class);
		job.setReducerClass(DeviceSimilaritySVMReducer.class);
		job.setNumReduceTasks(2);
		
		SequenceFileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		if (job.waitForCompletion(true)) {
			if (logger.isInfoEnabled()) {
				logger.info("Extracting is done.");
			}
			return 1;
		}
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res = -1;
		try {
			logger.info("Initializing Device Similarity SVM Trainning Job");
			DeviceSimilaritySVMTrainner deviceSimilarity = new DeviceSimilaritySVMTrainner();

			// Let ToolRunner handle generic command-line options and run hadoop
			res = ToolRunner.run(new Configuration(), deviceSimilarity, args);
			System.exit(res);
			logger.info("Device Similarity finished running hadoop");

		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			logger.info("Quitting with error code " + res);
			System.exit(res);
		}
	} 

	public static class DeviceSimilaritySVMMapper extends
		Mapper<LongWritable, List<Text>, Text, DeviceModel> {
		public static final Log logger = LogFactory.getLog(DeviceSimilaritySVMMapper.class);
		protected void map(LongWritable key, List<Text> values, Context context)
			throws IOException, InterruptedException { 
			if(values.size() < 450) {
				logger.info("######## Filter out Map Input values which has only :" + values.size() + " columns.");
				return;
			}
			String browserStringHash = values.get(13).toString();
			DeviceModel device = DeviceAttributeExtractor.getInstance().extractModel(values);
			context.write(new Text(browserStringHash), device);
		}
		
	}
	
	public static class DeviceSimilaritySVMReducer extends Reducer<Text, DeviceModel, NullWritable, Text> {
		private JaccardCoefficientSimilarity similarity = new JaccardCoefficientSimilarity();
		protected void reduce(Text key, Iterable<DeviceModel> values, Context context)
			throws IOException, InterruptedException {
			List<DeviceModel> lst = new ArrayList<DeviceModel> ();
			Iterator<DeviceModel> ite = values.iterator();
			while(ite.hasNext()) {
				DeviceModel dm = ite.next();
				lst.add(dm);
			}
			
			for (int i = 0; i < lst.size(); i++) {    
				DeviceModel dmX = lst.get(i);
				
				int nextIndex = i + 1;
				for (int j = 0; j < lst.size() - nextIndex; j++) {
					DeviceModel dmY = lst.get(j+nextIndex);
					double score = similarity.similarity(dmX.all(), dmY.all());
					double score_ba = similarity.similarity(dmX.getBrowserAttributes(), dmY.getBrowserAttributes());
					double score_pa = similarity.similarity(dmX.getPluginAttributes(), dmY.getPluginAttributes());
					double score_oa = similarity.similarity(dmX.getOsAttributes(), dmY.getOsAttributes());
					double score_ca = similarity.similarity(dmX.getConnectionAttributes(), dmY.getConnectionAttributes());
					
					//if(score <0.9 && score > 0.7)
						System.out
								.println(score
										+ "[browser:"
										+ score_ba
										+ " | plugin:"
										+ score_pa
										+ " | os:"
										+ score_oa
										+ " | connection:"
										+ score_ca
										+ "]"
										+ ":\n------------------------------------\n"
										+ dmX.toString()
										+ "\n"
										+ dmY.toString()
										+ "\n-------------------------------------------\n");
				}
			}
		}
	}
}
