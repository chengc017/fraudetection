/**
 * 
 */
package com.vormetric.fd.preprocess;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CSVLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CSVNLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author shawnguo
 *
 */
public class ValidValuePercentJob extends Configured implements Tool {
	public static final Log logger = LogFactory.getLog(ValidValuePercentJob.class);
	private static final String INPUT_PATH_PREFIX = "/Users/shawnkuo/Documents/fraudetection/csv/";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res = -1;
		try {
			logger.info("Initializing Transaction Preprocessor");
			ValidValuePercentJob preprocessor = new ValidValuePercentJob();

			// Let ToolRunner handle generic command-line options and run hadoop
			res = ToolRunner.run(new Configuration(), preprocessor,args);
			System.exit(res);
			logger.info("Transaction Preprocessor finished running hadoop");

		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			logger.info("Quitting with error code " + res);
			System.exit(res);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("Need input and output path to process the job");
			System.exit(1);
		}

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		Configuration conf = getConf();
		// Performance tuning
		conf.set("mapreduce.map.log.level", "ERROR");
		//1. using compression

		//2. reusing the JVM
		conf.setInt("mapred.job.reuse.jvm.num.tasks", 3);
		conf.set("output_path", out.toString());
		
		conf.set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
		conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
		conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
		
		conf.set("mapred.textoutputformat.separatorText", ",");
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)) {
			fs.delete(out, true);
			logger.info("delete exist output path");
		}
		
		Job job = new Job(conf);
		job.setJobName("Valid Value Percent");
		job.setJarByClass(ValidValuePercentJob.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(CSVNLineInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class); 
		
		job.setMapperClass(ValidValuePercentMapper.class);
		//job.setCombinerClass(ValidValuePercentReducer.class);
		job.setReducerClass(ValidValuePercentReducer.class);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		if (job.waitForCompletion(true)) {
			if (logger.isInfoEnabled()) {
				logger.info("Rate cal done.");
			}
			return 1;
		}
		return 0;
	}
	
	public static class ValidValuePercentMapper extends
			Mapper<LongWritable, List<Text>, IntWritable, Text> {
		protected void map(LongWritable key, List<Text> values, Context context)
			throws IOException, InterruptedException {
			String eventType = values.get(4).toString();
			if(eventType.equals("payment")) { //filter by event_type
				for(int i=0; i<values.size(); i++) {
					String val = values.get(i).toString();
					if(!val.equals("")) {
						context.write(new IntWritable(i), new Text("FILLED"));
					} 
				}
			}
		}
	}
	
	public static class ValidValuePercentReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			int filled_count = 0;
			for(Text value : values) {
				filled_count++;
			}
			double valid_rate = (double)filled_count/254.0;
			//if(valid_rate > 0.5)
				context.write(NullWritable.get(), new Text(key + "," + String.valueOf(valid_rate)));
		}
	}
}
