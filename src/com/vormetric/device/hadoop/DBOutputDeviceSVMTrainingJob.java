/**
 * 
 */
package com.vormetric.device.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CSVLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CSVTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vormetric.algorithm.similarities.JaccardCoefficientSimilarity;
import com.vormetric.device.extract.DeviceAttributeExtractor;
import com.vormetric.device.model.DeviceModel;

/**
 * @author xioguo
 *
 */
public class DBOutputDeviceSVMTrainingJob extends Configured implements Tool {

	public static final Log logger = LogFactory.getLog(DBOutputDeviceSVMTrainingJob.class);
	
	public static final String TABLENAME = "dd";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res = -1;
		try {
			logger.info("Initializing Device Similarity SVM Trainning Job");
			DBOutputDeviceSVMTrainingJob deviceSimilarity = new DBOutputDeviceSVMTrainingJob();

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

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 1) {
			System.out.println("Need input and output path to process the job");
			return 0;
		}
		
		Path in = new Path(args[0]);
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		// if table dose not exist, create one now.
		if(!admin.tableExists(TABLENAME)) {
			logger.info("creating training table...");
			boolean created = createTable(admin);
			if(!created) return 0;
		}
		conf.set(TableOutputFormat.OUTPUT_TABLE, TABLENAME);
		
		//Configuration conf = getConf();
		// Performance tuning
		//2. reusing the JVM
		conf.setInt("mapred.job.reuse.jvm.num.tasks", 3);
		
		conf.set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
		conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
		conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
		//
		conf.setBoolean("mapred.input.dir.recursive", true);
		
		Job job = new Job(conf);
		job.setJobName("Device Similarity SVM Trainner");
		job.setJarByClass(DBOutputDeviceSVMTrainingJob.class);
		
		//map's input format
		job.setInputFormatClass(CSVTextInputFormat.class);
		//reduce's output format
		job.setOutputFormatClass(TableOutputFormat.class);
		
		// map's output
		job.setOutputKeyClass(Text.class);  		
		job.setOutputValueClass(DeviceModel.class); 
		
		job.setMapperClass(DBOutputDeviceSVMTrainingMapper.class);
		job.setReducerClass(DBOutputDeviceSVMTrainingReducer.class);
		job.setNumReduceTasks(2);
		
		SequenceFileInputFormat.addInputPath(job, in);
		//FileOutputFormat.setOutputPath(job, out);
		
		if (job.waitForCompletion(true)) {
			if (logger.isInfoEnabled()) {
				logger.info("Extracting is done.");
			}
			return 1;
		}
		return 0;
	}
	
	private boolean createTable(HBaseAdmin admin) throws IOException {
		HTableDescriptor htd = new HTableDescriptor(TABLENAME);
		htd.addFamily(new HColumnDescriptor("h"));
		htd.addFamily(new HColumnDescriptor("t"));
		htd.addFamily(new HColumnDescriptor("s"));
		htd.addFamily(new HColumnDescriptor("d"));
		admin.createTable(htd);
		byte [] tablename = htd.getName();
		HTableDescriptor [] tables = admin.listTables();
		boolean result = false;
		int i=0;
		for(HTableDescriptor table : tables) {
			if(tables.length != 1 && Bytes.equals(tablename, tables[i++].getName())) {
				logger.info("Failed create of table.");
			} else {
				result = true;
			}
		}
		return result;
	}

	public static class DBOutputDeviceSVMTrainingMapper extends
		Mapper<LongWritable, List<Text>, Text, DeviceModel> {
		public static final Log logger = LogFactory.getLog(DBOutputDeviceSVMTrainingMapper.class);
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
	
	public static class DBOutputDeviceSVMTrainingReducer extends Reducer<Text, DeviceModel, NullWritable, Writable> {
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
							dmX.getBrowserAttributes(),
							dmY.getBrowserAttributes());
					double score_pa = similarity.similarity(
							dmX.getPluginAttributes(),
							dmY.getPluginAttributes());
					double score_oa = similarity.similarity(
							dmX.getOsAttributes(),
							dmY.getOsAttributes());
					double score_ca = similarity.similarity(
							dmX.getConnectionAttributes(),
							dmY.getConnectionAttributes());

					if(score <=0.8 && score >= 0.5) {
						byte [] rowID = Bytes.toBytes(String.valueOf(System.currentTimeMillis()));
						Put put = new Put(rowID);
						
						byte [] family = Bytes.toBytes("h");
						byte [] qualifier = Bytes.toBytes("string");
						byte [] value = Bytes.toBytes(key.toString());
						put.add(family, qualifier, value);
						
						family = Bytes.toBytes("t");
						qualifier = Bytes.toBytes("val");
						value = Bytes.toBytes(score >= 0.7 ? 1 : 0);
						put.add(family, qualifier, value);
						
						family = Bytes.toBytes("s");
						qualifier = Bytes.toBytes("total");
						value = Bytes.toBytes(score);
						put.add(family, qualifier, value);
						
						qualifier = Bytes.toBytes("browser");
						value = Bytes.toBytes(score_ba);
						put.add(family, qualifier, value);
						
						qualifier = Bytes.toBytes("plugin");
						value = Bytes.toBytes(score_pa);
						put.add(family, qualifier, value);
						
						qualifier = Bytes.toBytes("os");
						value = Bytes.toBytes(score_oa);
						put.add(family, qualifier, value);
						
						qualifier = Bytes.toBytes("conn");
						value = Bytes.toBytes(score_ca);
						put.add(family, qualifier, value);
						
						family = Bytes.toBytes("d");
						qualifier = Bytes.toBytes("X");
						value = Bytes.toBytes(dmX.toString().replaceAll("\\[|\\]", ""));
						put.add(family, qualifier, value);
						
						qualifier = Bytes.toBytes("Y");
						value = Bytes.toBytes(dmY.toString().replaceAll("\\[|\\]", ""));
						put.add(family, qualifier, value);
						
						context.write(NullWritable.get(), put);
					}
				}
			}
		}
	}
}
