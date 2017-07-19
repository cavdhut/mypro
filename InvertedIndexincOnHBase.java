/*
 * Assignment:  Implement Inverted Indexing using HBase 
 * Student Name:  Chandan Avdhut
 * 
 * 
 */
package onramp.hbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InvertedIndexincOnHBase extends Configured implements Tool {

	static String DOC = "DOC";
	static String TEXT = "Text";
	static String INDEX = "INDEX";

	static class InvertedIndexincOnHBaseMapper extends TableMapper<Text, Text> {

		@Override
		protected void map(ImmutableBytesWritable rowKey, Result postData,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String postText= Bytes.toString(postData.getValue(Bytes.toBytes(DOC), Bytes.toBytes(TEXT)));
			String postID= Bytes.toString(postData.getRow());
			StringTokenizer tokenizer = new StringTokenizer(postText, " \n\t\r,!*^$/?\\ /r");
			
			// For each word in the line emit a key-value record where 
			// the key is equal to the word and the value is equal to document ID
			while (tokenizer.hasMoreTokens()) {
				
				context.write(new Text(tokenizer.nextToken()), new Text(postID));
			}
			
			}
	}

	static class InvertedIndexincOnHBaseReducer<K> extends TableReducer<Text, Text, K> {

		@Override
		protected void reduce(Text word, Iterable<Text> intermedValues,
				Reducer<Text, Text, K, Mutation>.Context context) throws IOException, InterruptedException {

			// Add row for each word and column for each document.  Row keys are words and cell value is "1" if document 
			//has that word

			Put put = new Put(Bytes.toBytes(word.toString()));
			for (Text value : intermedValues) {
				
				put.addColumn(Bytes.toBytes(INDEX), Bytes.toBytes(value.toString()), Bytes.toBytes("1"));
				
			}


			context.write(null, put);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "InvertedIndexincOnHBase");

		job.setJarByClass(InvertedIndexincOnHBase.class);

		job.setMapperClass(InvertedIndexincOnHBaseMapper.class);
		job.setReducerClass(InvertedIndexincOnHBaseReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TableInputFormat.class);
		job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "brown");

		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "invertedindex");

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int runExitCode = ToolRunner.run(new InvertedIndexincOnHBase(), args);
		System.exit(runExitCode);
	}
}

