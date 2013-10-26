/**
 * 
 */
package com.vormetric.device;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
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
 * @author xioguo
 *
 */
public class SVMDeviceSimilarityTrainningJob extends Configured implements Tool {

	public static final Log logger = LogFactory.getLog(SVMDeviceSimilarityTrainningJob.class);
	
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
		job.setJarByClass(SVMDeviceSimilarityTrainningJob.class);
		
		job.setInputFormatClass(CSVTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// map's output
		job.setOutputKeyClass(Text.class);  		
		job.setOutputValueClass(DeviceModel.class); 
		
		job.setMapperClass(SVMDeviceSimilarityTrainningMapper.class);
		job.setReducerClass(SVMDeviceSimilarityTrainningReducer.class);
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
			SVMDeviceSimilarityTrainningJob deviceSimilarity = new SVMDeviceSimilarityTrainningJob();

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

	public static class SVMDeviceSimilarityTrainningMapper extends
		Mapper<LongWritable, List<Text>, Text, DeviceModel> {
		public static final Log logger = LogFactory.getLog(SVMDeviceSimilarityTrainningMapper.class);
		protected void map(LongWritable key, List<Text> values, Context context)
			throws IOException, InterruptedException { 
			if(values.size() < 450 || values.get(13).equals("")) {
				logger.info("######## Filter out Map Input values which has only :" + values.size() + " columns.");
				return;
			}
			String browserStringHash = values.get(13).toString();
			DeviceModel device = DeviceAttributeExtractor.getInstance().extractModel(values);
			context.write(new Text(browserStringHash), device);
		}
		
	}
	
	public static class SVMDeviceSimilarityTrainningReducer extends Reducer<Text, DeviceModel, NullWritable, Text> {
		private JaccardCoefficientSimilarity similarity = new JaccardCoefficientSimilarity();
		protected void reduce(Text key, Iterable<DeviceModel> values, Context context)
			throws IOException, InterruptedException {
			Vector<DeviceModel> v = new Vector<DeviceModel> ();
			Iterator<DeviceModel> ite = values.iterator();
			while(ite.hasNext()) {
				DeviceModel item = ite.next();
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
				DeviceModel dmX = (DeviceModel)v.get(i);
				
				int nextIndex = i + 1;
				for (int j = 0; j < v.size() - nextIndex; j++) {
					DeviceModel dmY = (DeviceModel)v.get(j+nextIndex);
					double score = similarity.similarity(dmX.all(), dmY.all());
					double score_ba = similarity.similarity(
							convert(dmX.getBrowserAttributes()),
							convert(dmY.getBrowserAttributes()));
					double score_pa = similarity.similarity(
							convert(dmX.getPluginAttributes()),
							convert(dmY.getPluginAttributes()));
					double score_oa = similarity.similarity(
							convert(dmX.getOsAttributes()),
							convert(dmY.getOsAttributes()));
					double score_ca = similarity.similarity(
							convert(dmX.getConnectionAttributes()),
							convert(dmY.getConnectionAttributes()));

					if(score <=0.8 && score >= 0.5) {
						String scoresLine = score + "[browser:"
								+ score_ba + " | plugin:"
								+ score_pa + " | os:"
								+ score_oa + " | connection:"
								+ score_ca + "],,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,";
						String deviceXLine = dmX.toString().replaceAll("\\[|\\]", "");
						String deviceYLine = dmY.toString().replaceAll("\\[|\\]", "") ;
						context.write(NullWritable.get(), new Text(scoresLine));
						context.write(NullWritable.get(), new Text(deviceXLine));
						context.write(NullWritable.get(), new Text(deviceYLine));
					}
					
				}
			}
		}
		
		private List<String> convert(List<Text> list) {
			List<String> strList = new LinkedList<String> ();
			for(Text item:list) {
				strList.add(item.toString());
			}
			return strList;
		}
	}
}
