/**
 * 
 */
package com.vormetric.device;

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
import org.apache.hadoop.mapreduce.lib.input.ArrayListTextWritable;
import org.apache.hadoop.mapreduce.lib.input.CSVLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CSVTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vormetric.algorithm.similarities.JaccardCoefficientSimilarity;
import com.vormetric.device.extract.DeviceAttributeExtractor;

/**
 * @author shawnkuo
 *
 */
public class SimpleDeviceSimilarityJob extends Configured implements Tool {

	public static final Log logger = LogFactory.getLog(SimpleDeviceSimilarityJob.class);
	
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
		job.setJarByClass(SimpleDeviceSimilarityJob.class);
		
		job.setInputFormatClass(CSVTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayListTextWritable.class); 
		
		job.setMapperClass(DeviceSimilarityMapper.class);
		job.setReducerClass(DeviceSimilarityReducer.class);
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
			SimpleDeviceSimilarityJob deviceSimilarity = new SimpleDeviceSimilarityJob();

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
	
	public static class DeviceSimilarityMapper extends
		Mapper<LongWritable, List<Text>, Text, List<Text>> {
		public static final Log logger = LogFactory.getLog(DeviceSimilarityMapper.class);
		protected void map(LongWritable key, List<Text> values, Context context)
			throws IOException, InterruptedException { 
			if(values.size() < 450) {
				logger.info("######## Filter out Map Input values which has only :" + values.size() + " columns.");
				return;
			}
			String browserStringHash = values.get(13).toString();
			List<Text> extracted = DeviceAttributeExtractor.getInstance().extractText(values);
			context.write(new Text(browserStringHash), extracted);
		}
		
	}
	
	public static class DeviceSimilarityReducer extends Reducer<Text, List<Text>, NullWritable, Text> {
		private JaccardCoefficientSimilarity similarity = new JaccardCoefficientSimilarity();
		protected void reduce(Text key, Iterable<List<Text>> values, Context context)
			throws IOException, InterruptedException {
			Vector<List<String>> v = new Vector<List<String>> ();
			for(List<Text> line : values) {
				List<String> vl = convert(line);
				v.add(vl);
			}
			
			for (int i = 0; i < v.size(); i++) {
				List<String> deviceX = v.get(i);
				
				int index = i + 1;
				for (int j = 0; j < v.size() - index; j++) {
					List<String> deviceY = v.get(j+index);
					double score = similarity.similarity(deviceX, deviceY);
					if(score <0.9 && score > 0.7)
						System.out.println(score + ":\n------------------------------------\n" + deviceX.toString() 
							+ "\n"+ deviceY.toString()
							+ "\n-------------------------------------------\n");
				}
			}
		}
		
		private List<String> convert(List<Text> lst) {
			List<String> strList = new ArrayList<String> ();
			for(Text text:lst) {
				strList.add(text.toString());
			}
			return strList;
		}
	}
}
