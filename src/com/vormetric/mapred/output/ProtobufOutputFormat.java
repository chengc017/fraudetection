/**
 * 
 */
package com.vormetric.mapred.output;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.protobuf.GeneratedMessage;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.vormetric.device.proto.DeviceProto.Device;

/**
 * @author shawnguo
 * 
 * output parsed data 
 */
@SuppressWarnings("unchecked")
public class ProtobufOutputFormat extends FileOutputFormat {

	private static final NumberFormat NUMBER_FORMATS = NumberFormat.getInstance();
	public static final Log log = LogFactory.getLog(ProtobufOutputFormat.class);
	
	static {
		NUMBER_FORMATS.setMinimumIntegerDigits(5);
		NUMBER_FORMATS.setGroupingUsed(false);
	}
	
	protected static class ProtobufRecordWriter
			extends RecordWriter {

		protected DataOutputStream out;
		
		protected ProtobufBlockWriter writer ;
		
		public ProtobufRecordWriter(DataOutputStream out) {
			this.out = out;
			writer = new ProtobufBlockWriter(
					out, GeneratedMessage.class);
		}
		
		@Override
		public synchronized void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			writer.finish();
			writer.close();
			out.close();
		}

		@Override
		public void write(Object key, Object value) throws IOException,
				InterruptedException {
			Object obj = ((ProtobufWritable)value).get();
			if(obj instanceof Device) {
				writer.write((Device)obj);
			}/* else if(obj instanceof PoiWrapper) {
				writer.write((PoiWrapper)obj);
			}*/
		}
	}
	
	@Override
	public RecordWriter getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Path path = FileOutputFormat.getOutputPath(context);
		Path file = new Path(path.toString() + File.separator + getFile(context));
        FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataOutputStream outputStream = fs.create(file, false);
		return new ProtobufRecordWriter(outputStream);
	}
	
	protected synchronized String getFile(TaskAttemptContext context) {
		TaskID taskId = context.getTaskAttemptID().getTaskID();
	    int partition = taskId.getId();
	    StringBuilder result = new StringBuilder();
	    result.append("part");
	    result.append('-');
	    result.append(NUMBER_FORMATS.format(partition));
	    result.append(".pb");
	    return result.toString();
	}
	
}
