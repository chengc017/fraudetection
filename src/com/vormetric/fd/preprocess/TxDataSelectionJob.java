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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CSVLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author shawnkuo
 *
 */
@SuppressWarnings("deprecation")
public class TxDataSelectionJob extends Configured implements Tool {

	public static final Log logger = LogFactory.getLog(TxDataSelectionJob.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res = -1;
		try {
			logger.info("Initializing Transaction Data Selection Job");
			TxDataSelectionJob txselection = new TxDataSelectionJob();

			// Let ToolRunner handle generic command-line options and run hadoop
			res = ToolRunner.run(new Configuration(), txselection,args);
			System.exit(res);
			logger.info("Transaction Selection finished running hadoop");

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
		
		// delete exist output path
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)) {
			fs.delete(out, true);
			logger.info("delete exist output");
		}
		
		// create job
		Job job = new Job(conf);
		job.setJobName("Transaction Preprocessing");
		job.setJarByClass(TxDataSelectionJob.class);
		job.setNumReduceTasks(3);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class); 
		
		job.setMapperClass(TxDataSelectionMapper.class);
		job.setReducerClass(TxDataSelectionReducer.class);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		if (job.waitForCompletion(true)) {
			if (logger.isInfoEnabled()) {
				logger.info("Tx Selection done.");
			}
			return 1;
		}
		
		return 0;
	}
	
	public static class TxDataSelectionMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
		public static final Log logger = LogFactory.getLog(TxDataSelectionMapper.class);
		
		protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			String [] lineCloumns = value.toString().split(",");
			if(lineCloumns.length < 405) {
				logger.info("######## Filter out invalid line that has " + lineCloumns.length + " clomuns only.");
				return;
			}
			String eventType = lineCloumns[4].toString();
			StringBuffer linebuf = new StringBuffer();
			if(eventType.equals("payment")) { //filter by event_type
				 linebuf.append(lineCloumns[26]).append(",") 		//device_match_result
						.append(lineCloumns[27]).append(",")		//device_result
						.append(lineCloumns[30]).append(",")		//enabled_ck
						.append(lineCloumns[31]).append(",")		//enabled_fl
						.append(lineCloumns[32]).append(",")		//enabled_im
						.append(lineCloumns[33]).append(",")		//enabled_js
						.append(lineCloumns[50]).append(",") 		//image_loaded
						.append(lineCloumns[65]).append(",")		//screen_res	82.3%
						//.append(lineCloumns[70]).append(",")		//tcp_os_signature	63%
						//.append(lineCloumns[71]).append(",")		//tcp_os_sig_raw	64%
						//.append(lineCloumns[72]).append(",")		//tcp_tstmp_rate		26%
						//.append(lineCloumns[73]).append(",")		//third_party_cookie	66.5%
						.append(lineCloumns[91]).append(",")		//ua_browser	91%	
						.append(lineCloumns[92]).append(",")		//ua_mobile		15%	
						.append(lineCloumns[93]).append(",")		//ua_os		87%
						//.append(lineCloumns[94]).append(",")		//ua_platform	86%
						.append(lineCloumns[101]).append(",")		//request_duration	100%
						.append(lineCloumns[109]).append(",")		//mime_type_number	52%
						.append(lineCloumns[121]).append(",")		//fuzzy_device_id_confidence
						.append(lineCloumns[123]).append(",")		//fuzzy_device_match_result
						.append(lineCloumns[131]).append(",")		//fuzzy_device_result
						//.append(lineCloumns[154]).append(",")		//true_ip_isp
						.append(lineCloumns[163]).append(",")		//true_ip_score
						.append(lineCloumns[164]).append(",")		//true_ip_worst_score
						.append(lineCloumns[171]).append(",")		//proxy_ip
						.append(lineCloumns[180]).append(",")		//proxy_ip_result
						.append(lineCloumns[181]).append(",")		//proxy_ip_score
						.append(lineCloumns[182]).append(",")		//proxy_ip_worst_score
						.append(lineCloumns[183]).append(",")		//proxy_type
						.append(lineCloumns[193]).append(",")		//account_name_result
						.append(lineCloumns[194]).append(",")		//account_name_score
						.append(lineCloumns[195]).append(",")		//account_name_worst_score
						.append(lineCloumns[204]).append(",")		//account_login_result
						.append(lineCloumns[215]).append(",")		//password_hash_result
						.append(lineCloumns[226]).append(",")		//account_number_result
						.append(lineCloumns[272]).append(",")		//account_email_result
						.append(lineCloumns[284]).append(",")		//account_address_result	16.1%
						.append(lineCloumns[339]).append(",")		//transaction_amount	19.7%
						.append(lineCloumns[342]).append(",")		//transaction_currency	18.5%
						.append(lineCloumns[387]).append(",")		//tcp_os_sig_adv_mss	63.8%
						.append(lineCloumns[388]).append(",")		//tcp_os_sig_snd_mss	63.4%
						.append(lineCloumns[389]).append(",")		//tcp_os_sig_rcv_mss	63.4%
						.append(lineCloumns[390]).append(",")		//http_os_sig_adv_mss	88.2%
						.append(lineCloumns[391]).append(",")		//http_os_sig_snd_mss	88.2%
						.append(lineCloumns[392]).append(",")		//http_os_sig_rcv_mss	88.2%
						.append(lineCloumns[393]).append(",")		//tcp_os_sig_ttl	63.4%
						.append(lineCloumns[394]).append(",")		//http_os_sig_ttl	88.2%
						.append(lineCloumns[395]).append(",")		//profiling_delta	88.2%
						.append(lineCloumns[396]).append(",")		//profiling_site_id		88.2%
						.append(lineCloumns[405]);				//http_referer_domain_result	73%
				 		// 44 cols
						context.write(key, new Text(linebuf.toString()));
			}
		}
	}
	
	public static class TxDataSelectionReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			for(Text line : values) {
				String txline = line.toString();
				String [] splits = txline.split(",");
				StringBuffer replacedBuff = new StringBuffer();
				int i = 0;
				for(String col:splits) {
					replacedBuff.append(i > 0 ? "," : "");
					replacedBuff.append(col.equals("") ? "-99" : col);
					i++;
				}
				context.write(NullWritable.get(), new Text(replacedBuff.toString()));
			}
		}
	}
}
