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
import org.apache.hadoop.mapreduce.lib.input.CSVNLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
		
		job.setInputFormatClass(CSVNLineInputFormat.class);
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
		Mapper<LongWritable, List<Text>, LongWritable, Text> {
		public static final Log logger = LogFactory.getLog(TxDataSelectionMapper.class);
		
		protected void map(LongWritable key, List<Text> values, Context context)
			throws IOException, InterruptedException {
			if(values.size() < 405) {
				logger.info("######## Map Input values size:" + values.size());
				return;
			}
			String eventType = values.get(4).toString();
			StringBuffer linebuf = new StringBuffer();
			if(eventType.equals("payment")) { //filter by event_type
				 linebuf.append(values.get(26).toString()).append(",") 		//device_match_result
						.append(values.get(27).toString()).append(",")		//device_result
						.append(values.get(30).toString()).append(",")		//enabled_ck
						.append(values.get(31).toString()).append(",")		//enabled_fl
						.append(values.get(32).toString()).append(",")		//enabled_im
						.append(values.get(33).toString()).append(",")		//enabled_js
						.append(values.get(50).toString()).append(",") 		//image_loaded
						.append(values.get(65).toString()).append(",")		//screen_res	82.3%
						//.append(values.get(70).toString()).append(",")		//tcp_os_signature	63%
						//.append(values.get(71).toString()).append(",")		//tcp_os_sig_raw	64%
						//.append(values.get(72).toString()).append(",")		//tcp_tstmp_rate		26%
						//.append(values.get(73).toString()).append(",")		//third_party_cookie	66.5%
						.append(values.get(91).toString()).append(",")		//ua_browser	91%	
						.append(values.get(92).toString()).append(",")		//ua_mobile		15%	
						.append(values.get(93).toString()).append(",")		//ua_os		87%
						//.append(values.get(94).toString()).append(",")		//ua_platform	86%
						.append(values.get(101).toString()).append(",")		//request_duration	100%
						.append(values.get(109).toString()).append(",")		//mime_type_number	52%
						.append(values.get(121).toString()).append(",")		//fuzzy_device_id_confidence
						.append(values.get(123).toString()).append(",")		//fuzzy_device_match_result
						.append(values.get(131).toString()).append(",")		//fuzzy_device_result
						//.append(values.get(154).toString()).append(",")		//true_ip_isp
						.append(values.get(163).toString()).append(",")		//true_ip_score
						.append(values.get(164).toString()).append(",")		//true_ip_worst_score
						.append(values.get(171).toString()).append(",")		//proxy_ip
						.append(values.get(180).toString()).append(",")		//proxy_ip_result
						.append(values.get(181).toString()).append(",")		//proxy_ip_score
						.append(values.get(182).toString()).append(",")		//proxy_ip_worst_score
						.append(values.get(183).toString()).append(",")		//proxy_type
						.append(values.get(193).toString()).append(",")		//account_name_result
						.append(values.get(194).toString()).append(",")		//account_name_score
						.append(values.get(195).toString()).append(",")		//account_name_worst_score
						.append(values.get(204).toString()).append(",")		//account_login_result
						.append(values.get(215).toString()).append(",")		//password_hash_result
						.append(values.get(226).toString()).append(",")		//account_number_result
						.append(values.get(272).toString()).append(",")		//account_email_result
						.append(values.get(284).toString()).append(",")		//account_address_result	16.1%
						.append(values.get(339).toString()).append(",")		//transaction_amount	19.7%
						.append(values.get(342).toString()).append(",")		//transaction_currency	18.5%
						.append(values.get(387).toString()).append(",")		//tcp_os_sig_adv_mss	63.8%
						.append(values.get(388).toString()).append(",")		//tcp_os_sig_snd_mss	63.4%
						.append(values.get(389).toString()).append(",")		//tcp_os_sig_rcv_mss	63.4%
						.append(values.get(390).toString()).append(",")		//http_os_sig_adv_mss	88.2%
						.append(values.get(391).toString()).append(",")		//http_os_sig_snd_mss	88.2%
						.append(values.get(392).toString()).append(",")		//http_os_sig_rcv_mss	88.2%
						.append(values.get(393).toString()).append(",")		//tcp_os_sig_ttl	63.4%
						.append(values.get(394).toString()).append(",")		//http_os_sig_ttl	88.2%
						.append(values.get(395).toString()).append(",")		//profiling_delta	88.2%
						.append(values.get(396).toString()).append(",")		//profiling_site_id		88.2%
						.append(values.get(405).toString());				//http_referer_domain_result	73%
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
