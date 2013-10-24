/**
 * 
 */
package com.vormetric.mapred.output;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author shawnkuo
 *
 */
public class MultipleOutputProtobufFormat extends ProtobufOutputFormat {

	protected synchronized String _getFile(TaskAttemptContext context) {
		String file = null;
		try {
			Path filePath = getDefaultWorkFile(context, ".pb");
			int lastIndex = filePath.toString().lastIndexOf("/");
			file = filePath.toString().substring(lastIndex+1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String directory = file.substring(0, file.indexOf("-"));
	    return directory + File.separator + file;
	}
	
	protected synchronized String getFile(TaskAttemptContext context) {
		String file = null;
		try {
			Path filePath = getDefaultWorkFile(context, ".pb");
			int lastIndex = filePath.toString().lastIndexOf("/");
			file = filePath.toString().substring(lastIndex+1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	    return file;
	}
}
