/**
 * 
 */
package com.vormetric.device.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

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
 * @author shawnkuo
 *
 */
public class TextOutputDeviceSVMTrainingJob extends Configured implements Tool {

	public static final Log logger = LogFactory.getLog(TextOutputDeviceSVMTrainingJob.class);
	
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
		job.setJobName("Device Similarity");
		job.setJarByClass(TextOutputDeviceSVMTrainingJob.class);
		
		job.setInputFormatClass(CSVTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DeviceModel.class); 
		
		job.setMapperClass(TextOutputDeviceSVMTrainingMapper.class);
		job.setReducerClass(TextOutputDeviceSVMTrainingReducer.class);
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
			logger.info("Initializing Device Similarity Job");
			TextOutputDeviceSVMTrainingJob deviceSimilarity = new TextOutputDeviceSVMTrainingJob();

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
	
	public static class TextOutputDeviceSVMTrainingMapper extends
		Mapper<LongWritable, List<Text>, Text, DeviceModel> {
		public static final Log logger = LogFactory.getLog(TextOutputDeviceSVMTrainingMapper.class);
		protected void map(LongWritable key, List<Text> values, Context context)
			throws IOException, InterruptedException { 
			if(values.size() < 450) {
				logger.info("######## Filter out Map Input values which has only :" + values.size() + " columns.");
				return;
			}
			String browserStringHash = values.get(13).toString();
			if(browserStringHash.equals("")) return;
			DeviceModel device = DeviceAttributeExtractor.getInstance().extractModel(values);
			context.write(new Text(browserStringHash), device);
		}
	}
	
	public static class TextOutputDeviceSVMTrainingReducer extends Reducer<Text, DeviceModel, NullWritable, Text> {
		private JaccardCoefficientSimilarity similarity = new JaccardCoefficientSimilarity();
		protected void reduce(Text key, Iterable<DeviceModel> values, Context context)
			throws IOException, InterruptedException {
			Vector<DeviceModel> v = new Vector<DeviceModel> ();
			for(DeviceModel item : values) {
				DeviceModel dm = new DeviceModel(item.getOrgId(),
						item.getEventId(), item.getRequestId(),
						item.getDeviceMatchResult(), item.getSessionId());
				dm.setBrowserAttributes(item.getBrowserAttributes());
				dm.setPluginAttributes(item.getPluginAttributes());
				dm.setOsAttributes(item.getOsAttributes());
				dm.setConnectionAttributes(item.getConnectionAttributes());
				v.add(dm);
			}
			
			for (int i = 0; i < v.size(); i++) {
				DeviceModel deviceX = v.get(i);
				
				int index = i + 1;
				for (int j = 0; j < v.size() - index; j++) {
					DeviceModel deviceY = v.get(j+index);
					double score = similarity.similarity(deviceX.all(), deviceY.all());
					double browser = similarity.similarity(
							deviceX.getBrowserAttributes(),
							deviceY.getBrowserAttributes());
					double plugin = similarity.similarity(
							deviceX.getPluginAttributes(),
							deviceY.getPluginAttributes());
					double os = similarity.similarity(
							deviceX.getOsAttributes(),
							deviceY.getOsAttributes());
					double connection = similarity.similarity(
							deviceX.getConnectionAttributes(),
							deviceY.getConnectionAttributes());
					
					int label = 0;
					if(score >= 0.8) {
						label = 1;
					} 
					
					String svmline = label + " 1:" + browser + " 2:"+ plugin + " 3:"+os + " 4:"+connection;
					String output = 
							+ score + " [browser:"+ browser + " | plugin:" + plugin + " | os:" + os + " | connection:" + connection + "]"
							+ "\n-------------------------------------------\n" 
							+ deviceX.getBrowserAttributes().toString() + "\n"
							+ deviceY.getBrowserAttributes().toString() + "\n\n"
							+ deviceX.getPluginAttributes().toString() + "\n"
							+ deviceY.getPluginAttributes().toString() + "\n\n"
							+ deviceX.getOsAttributes().toString() + "\n"
							+ deviceY.getOsAttributes().toString() + "\n\n"
							+ deviceX.getConnectionAttributes().toString() + "\n"
							+ deviceY.getConnectionAttributes().toString() + "\n"
							+ "-------------------------------------------\n";
					System.out.println(svmline);
					//if(score <= 0.8 && score >= 0.6) {
						context.write(NullWritable.get(), new Text(svmline));
					//}
				}
			}
		}
	}
}
