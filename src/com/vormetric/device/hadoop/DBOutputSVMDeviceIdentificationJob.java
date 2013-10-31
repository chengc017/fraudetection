/**
 * 
 */
package com.vormetric.device.hadoop;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vormetric.algorithm.DIHelper;
import com.vormetric.algorithm.decision.Match;
import com.vormetric.algorithm.decision.SVMDeviceSimilarityDecision;
import com.vormetric.algorithm.similarities.JaccardCoefficientSimilarity;
import com.vormetric.device.extract.DeviceAttributeExtractor;
import com.vormetric.device.hbase.DeviceDAO;
import com.vormetric.device.model.DeviceModel;
import com.vormetric.device.utils.ConfigUtil;
import com.vormetric.mapred.csv.CSVInputFormat;

/**
 * @author shawnkuo
 *
 */
public class DBOutputSVMDeviceIdentificationJob extends Configured implements
		Tool {

	public static final Log logger = LogFactory.getLog(DBOutputSVMDeviceIdentificationJob.class);
	
	public static final String TABLENAME = "devices";
	
	public static HBaseAdmin admin = null;
	
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
		conf.set("hbase.zookeeper.quorum", ConfigUtil.getHbaseHost());
		
		admin = new HBaseAdmin(conf);
		// if table dose not exist, create one now.
		if(!admin.tableExists(DeviceDAO.TABLE_NAME)) {
			logger.info("creating training table...");
			boolean created = createTable(admin);
			if(!created) return 0;
		}
		conf.set(TableOutputFormat.OUTPUT_TABLE, Bytes.toString(DeviceDAO.TABLE_NAME));
		
		//Configuration conf = getConf();
		// Performance tuning
		//2. reusing the JVM
		conf.setInt("mapred.job.reuse.jvm.num.tasks", 3);
		
		conf.setBoolean("mapred.input.dir.recursive", true);
		
		Job job = new Job(conf);
		job.setJobName("Device Similarity SVM Trainner");
		job.setJarByClass(DBOutputSVMDeviceIdentificationJob.class);
		
		//map's input format
		job.setInputFormatClass(CSVInputFormat.class);
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
		HTableDescriptor htd = new HTableDescriptor(DeviceDAO.TABLE_NAME);
		htd.addFamily(new HColumnDescriptor(DeviceDAO.HASH_FAMILY)); //browser string hash value
		htd.addFamily(new HColumnDescriptor(DeviceDAO.LENGTH_FAMILY)); //numbers of similar devices merged in this row.
		htd.addFamily(new HColumnDescriptor(DeviceDAO.DEVICE_FAMILY)); //devices family
		htd.addFamily(new HColumnDescriptor(DeviceDAO.TRANSACTION_FAMILY)); //transaction
		admin.createTable(htd);
		byte [] tablename = htd.getName();
		HTableDescriptor [] tables = admin.listTables();
		boolean result = false;
		for(HTableDescriptor table : tables) {
			if(tables.length != 1 && Bytes.equals(tablename, table.getName())) {
				logger.info("Failed create of table.");
			} else {
				result = true;
			}
		}
		return result;
	}
	
	public static class DBOutputSVMDeviceIdentificationMapper extends
		Mapper<LongWritable, List<Text>, Text, DeviceModel> {
		public static final Log logger = LogFactory
				.getLog(DBOutputSVMDeviceIdentificationMapper.class);
		protected void map(LongWritable key, List<Text> values, Context context)
			throws IOException, InterruptedException { 
			if (values.size() < 450) {
				logger.info("######## Filter out Map Input values which has only :"
						+ values.size() + " columns.");
				return;
			}
			String broserHash = values.get(13).toString();
			if(broserHash.equals("")) return;
			
			DeviceModel device = DeviceAttributeExtractor.getInstance()
					.extractModel(values);
			context.write(new Text(broserHash), device);
		}
	} 
	
	public static class DBOutputSVMDeviceIdentificationReducer extends
			Reducer<Text, DeviceModel, NullWritable, Writable> {
		public static final Log logger = LogFactory
				.getLog(DBOutputSVMDeviceIdentificationReducer.class);
		private JaccardCoefficientSimilarity similarity = new JaccardCoefficientSimilarity();
		private SVMDeviceSimilarityDecision decision = new SVMDeviceSimilarityDecision(
				similarity);
		private DeviceDAO dao = null;
		
		@Override
		protected void setup(Context context) {
			dao = new DeviceDAO(admin);
		}
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
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
				dm.setBrowserHash(item.getBrowserHash());
				v.add(dm);
			}
			
			Match match = new Match(false);
			for (int i = 0; i < v.size(); i++) {
				Object buddy = null;
				Object dmX = v.get(i);
				for (int j = 0; j < duplicates.size(); j++) {
					Object dmY = duplicates.get(j);
					match = decision.match(dmX, dmY);

					if(match.result) {
						buddy = dmY;
						break;
					}
				}
				
				if (buddy == null) {
					duplicates.add(dmX);
				} else {
					Object unioned = null;
					if(match.result && match.score != 1) {
						unioned = DIHelper.union(dmX, buddy);
					} else {
						unioned = dmX;
					}
					duplicates.remove(buddy);
					v.add(unioned);
				}
			}
			
			for(int k=0; k<duplicates.size(); k++) {
				//db
				write(duplicates.get(k), context);
			}
		}
		
		@SuppressWarnings({ "unchecked", "static-access" })
		private void write(Object obj, Context context) throws IOException,
				InterruptedException {
			if (obj instanceof DeviceModel) {
				DeviceModel d = (DeviceModel) obj;
				//for individual device, create unique row key default.  
				Put p = dao.mkPut(d);
				context.write(NullWritable.get(), p);
			} else {
				List<DeviceModel> lst = (ArrayList<DeviceModel>) obj;
				long dt = System.currentTimeMillis();
				for (int i = 0; i < lst.size(); i++) { 
					//for those devices unioned together, will be inserted in the same row, t
					//hus we use browser hash & timestamp to 
					//create a same row key for them.
					Put p = dao.mkPut(lst.get(i),
							dao.mkRowKey(lst.get(i).getBrowserHash(), dt),
							lst.size(),
							i, i);
					context.write(NullWritable.get(), p);
				}
			}
		}
	}
}
