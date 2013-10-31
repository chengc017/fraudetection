/**
 * 
 */
package com.vormetric.mapred.csv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import au.com.bytecode.opencsv.CSVParser;

/**
 * @author xioguo
 * 
 */
public class CSVInputFormat extends
		FileInputFormat<LongWritable, List<Text>> {

	public static String CSV_TOKEN_SEPARATOR_CONFIG = "csvinputformat.token.delimiter";

	@Override
	public RecordReader<LongWritable, List<Text>> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		String csvDelimiter = context.getConfiguration().get( // <co
																			// id="ch02_comment_csv_inputformat1"/>
				CSV_TOKEN_SEPARATOR_CONFIG);

		Character separator = null;
		if (csvDelimiter != null && csvDelimiter.length() == 1) {
			separator = csvDelimiter.charAt(0);
		}

		return new CSVRecordReader(separator); // <co
												// id="ch02_comment_csv_inputformat2"/>
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return true;
	}

	public static class CSVRecordReader // <co
										// id="ch02_comment_csv_inputformat4"/>
			extends RecordReader<LongWritable, List<Text>> {
		private LineRecordReader reader;
		private List<Text> value;
		private final CSVParser parser;

		public CSVRecordReader(Character csvDelimiter) {
			this.reader = new LineRecordReader();
			if (csvDelimiter == null) {
				parser = new CSVParser(); // <co
											// id="ch02_comment_csv_inputformat5"/>
			} else {
				parser = new CSVParser(csvDelimiter);
			}
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context); // <co
												// id="ch02_comment_csv_inputformat6"/>
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (reader.nextKeyValue()) { // <co
											// id="ch02_comment_csv_inputformat7"/>
				loadCSV(); // <co id="ch02_comment_csv_inputformat8"/>
				return true;
			} else {
				value = null;
				return false;
			}
		}

		private void loadCSV() throws IOException { // <co
													// id="ch02_comment_csv_inputformat9"/>
			String line = reader.getCurrentValue().toString();
			String[] tokens = parser.parseLine(line); // <co
														// id="ch02_comment_csv_inputformat10"/>
			value = convert(tokens);
		}

		private List<Text> convert(String[] s) {
			List<Text> tlist = new ArrayList<Text>();
			for (int i = 0; i < s.length; i++) {
				tlist.add(new Text(s[i]));
			}
			return tlist;
		}

		@Override
		public LongWritable getCurrentKey() // <co
											// id="ch02_comment_csv_inputformat11"/>
				throws IOException, InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public List<Text> getCurrentValue() // <co
													// id="ch02_comment_csv_inputformat12"/>
				throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}
	}
}