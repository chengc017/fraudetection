/**
 * 
 */
package com.vormetric.device;

import java.io.IOException;
import java.util.Iterator;
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

import com.vormetric.algorithm.DIHelper;
import com.vormetric.algorithm.decision.SVMDeviceSimilarityDecision;
import com.vormetric.algorithm.similarities.JaccardCoefficientSimilarity;
import com.vormetric.device.extract.DeviceAttributeExtractor;
import com.vormetric.device.model.DeviceModel;

/**
 * @author shawnkuo
 *
 */
public class DBOutputSVMDeviceIdentificationJob extends Configured implements
		Tool {

	public static final Log logger = LogFactory.getLog(DBOutputSVMDeviceIdentificationJob.class);
	
	public static final String TABLENAME = "devices";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res = -1;
		try {
			logger.info("Initializing SVM Device Identification Job");
			DBOutputSVMDeviceIdentificationJob deviceIdentification = new DBOutputSVMDeviceIdentificationJob();

			// Let ToolRunner handle generic command-line options and run hadoop
			res = ToolRunner.run(new Configuration(), deviceIdentification, args);
			System.exit(res);
			logger.info("SVM Device Identification finished running hadoop");

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
		job.setJarByClass(DBOutputSVMDeviceIdentificationJob.class);
		
		//map's input format
		job.setInputFormatClass(CSVTextInputFormat.class);
		//reduce's output format
		job.setOutputFormatClass(TableOutputFormat.class);
		
		// map's output
		job.setOutputKeyClass(Text.class);  		
		job.setOutputValueClass(DeviceModel.class); 
		
		job.setMapperClass(DBOutputSVMDeviceIdentificationMapper.class);
		job.setReducerClass(DBOutputSVMDeviceIdentificationReducer.class);
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
		htd.addFamily(new HColumnDescriptor("h")); //browser string hash value
		htd.addFamily(new HColumnDescriptor("d")); //devices family
		htd.addFamily(new HColumnDescriptor("t")); //transaction
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
	
	public static class DBOutputSVMDeviceIdentificationMapper extends
		Mapper<LongWritable, List<Text>, Text, DeviceModel> {
		public static final Log logger = LogFactory.getLog(DBOutputSVMDeviceIdentificationMapper.class);
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
	
	public static class DBOutputSVMDeviceIdentificationReducer extends Reducer<Text, DeviceModel, NullWritable, Writable> {
		private JaccardCoefficientSimilarity similarity = new JaccardCoefficientSimilarity();
		private SVMDeviceSimilarityDecision decision = new SVMDeviceSimilarityDecision(
				similarity);
		@SuppressWarnings("unchecked")
		protected void reduce(Text key, Iterable<DeviceModel> values, Context context)
			throws IOException, InterruptedException {
			Vector v = new Vector ();
			Vector duplicates = new Vector();
			
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
				Object buddy = null;
				Object dmX = v.get(i);
				
				for (int j = 0; j < duplicates.size(); j++) {
					Object dmY = duplicates.get(j);
					boolean match = decision.match(dmX, dmY);

					if(match) {
						buddy = dmY;
						break;
					}
				}
				
				if (buddy == null) {
					duplicates.add(dmX);
				} else {
					Object unioned = DIHelper.union(dmX, buddy);
					duplicates.remove(buddy);
					v.add(unioned);
				}
			}
			
			
			for(int k=0; k<duplicates.size(); k++) {
				context.getCounter("Device Identification", "Number of Devices").increment(1);
				if(duplicates.get(k) instanceof List<?>) {
					context.getCounter("Device Identification", "Number of Merged Devices").increment(1);
				}
				
				String timestamp = String.valueOf(System.currentTimeMillis());
				byte [] rowid = Bytes.toBytes(String.valueOf(timestamp));
				
				write(rowid, key.toString(), duplicates.get(k), context);
			}
		}
		
		private void write(byte[] rowid, String bh, Object obj, Context context) throws IOException, InterruptedException {
			if(obj instanceof DeviceModel) {
				write(rowid, bh, (DeviceModel)obj, "device-0", "tx-0" ,context);
			} else {
				List<DeviceModel> lst = (List<DeviceModel>) obj;
				int i = 0;
				for(DeviceModel dm : lst) {
					write(rowid, bh, dm, "device-"+i, "tx-"+i, context);
					i++;
				}
			}
		}
		
		private void write(byte[] rowid, String bh, DeviceModel dm,
				String deviceIndex, String trainsactionIndex, Context context) throws IOException,
				InterruptedException {

			Put put = new Put(rowid);

			byte[] family = Bytes.toBytes("h");
			byte[] qualifier = Bytes.toBytes("browser_hash");
			byte[] value = Bytes.toBytes(bh);
			put.add(family, qualifier, value);

			family = Bytes.toBytes("d");
			qualifier = Bytes.toBytes(deviceIndex);
			StringBuffer deviceStr = new StringBuffer();
			deviceStr.append(dm.getBrowserAttributes().toString()).append(",")
					.append(dm.getPluginAttributes().toString()).append(",")
					.append(dm.getOsAttributes().toString()).append(",")
					.append(dm.getConnectionAttributes().toString());
			value = Bytes.toBytes(deviceStr.toString());
			put.add(family, qualifier, value);

			family = Bytes.toBytes("t");
			qualifier = Bytes.toBytes(trainsactionIndex);
			String tx = dm.getOrgId() + "," + dm.getEventId() + ","
					+ dm.getRequestId() + "," + dm.getDeviceMatchResult() + ","
					+ dm.getSessionId();
			value = Bytes.toBytes(tx);
			put.add(family, qualifier, value);

			context.write(NullWritable.get(), put);
		}
	}
}
