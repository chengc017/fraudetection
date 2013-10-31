/**
 * 
 */
package com.vormetric.device.hadoop;

import java.io.IOException;
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
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CSVLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.vormetric.device.extract.DeviceAttributeExtractor;
import com.vormetric.device.proto.DeviceProto.Device;
import com.vormetric.fd.preprocess.TxDataSelectionJob.TxDataSelectionMapper;
import com.vormetric.mapred.csv.CSVInputFormat;
import com.vormetric.mapred.io.ProtobufDeviceWritable;
import com.vormetric.mapred.output.ProtobufOutputFormat;

/**
 * @author shawnkuo
 * step1: extract data from csv file, transfer to protobuf object.
 */
public class DeviceDataExtractorJob extends Configured implements Tool {

	public static final Log logger = LogFactory.getLog(DeviceDataExtractorJob.class);
	
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
		//1. using compression //got exception on windows during develop
		conf.setBoolean("mapred.compress.map.output", true);
		conf.setClass("mapred.map.output.compression.codec",
                BZip2Codec.class, CompressionCodec.class);
		//2. reusing the JVM
		conf.setInt("mapred.job.reuse.jvm.num.tasks", 3);
		conf.set("output_path", out.toString());
		
		//
		conf.setBoolean("mapred.input.dir.recursive", true);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)) {
			fs.delete(out, true);
		}
		
		Job job = new Job(conf);
		job.setJobName("Extract Device Attribute");
		job.setJarByClass(DeviceDataExtractorJob.class);
		
		job.setInputFormatClass(CSVInputFormat.class);
		job.setOutputFormatClass(ProtobufOutputFormat.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(ProtobufWritable.class); 
		
		job.setMapperClass(DeviceDataExtractorMapper.class);
		job.setReducerClass(DeviceDataExtractorReducer.class);
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
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DeviceDataExtractorJob(),args);
		System.exit(res);
	}

	public static class DeviceDataExtractorMapper extends
		Mapper<LongWritable, List<Text>, LongWritable, ProtobufWritable<Device>> {
		public static final Log logger = LogFactory.getLog(TxDataSelectionMapper.class);
		protected void map(LongWritable key, List<Text> values, Context context)
			throws IOException, InterruptedException {
			if(values.size() < 450) {
				logger.info("######## Filter out Map Input values which has only :" + values.size() + " columns.");
				return;
			}
			Device device = DeviceAttributeExtractor.getInstance().extract(values);
			ProtobufWritable<Device> protoWritable = ProtobufWritable.newInstance(Device.class);
			protoWritable.set(device);
			context.write(key, protoWritable);
		}
	}
	
	public static class DeviceDataExtractorReducer extends
		Reducer<LongWritable, ProtobufWritable<Device>, NullWritable, ProtobufDeviceWritable> {
		public static final Log log = LogFactory.getLog(DeviceDataExtractorReducer.class);
		public void reduce(LongWritable key, Iterable<ProtobufWritable<Device>> values, Context context)
				throws IOException, InterruptedException {
			for(ProtobufWritable<Device> value : values) {
				ProtobufDeviceWritable pw = new ProtobufDeviceWritable();
				value.setConverter(Device.class);
				pw.set(value.get());
				context.write(NullWritable.get(), pw);
			}
		}
	}

}
