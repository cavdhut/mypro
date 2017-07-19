/****************************************************************************************
 * Assignment:  Graph Processing Assignment 
 * Develop a MapReduce program that takes the output of the ConnectedComponents program and then writes each connected component as a sequence of node IDs in a text file (the output may be spread over more than one text files). 
 * For each connected component there should be a line in the text file where the node IDs of the component are listed and separated by comma from each other. You need to upload a Java source code file named CCFinalRound.java.
 * 
 * Sample Input file contents:
 * 1	1:2,3
 * 2	1:1,3
 * 3	1:1,2
 * 4	4:5,6,7
 * 5	4:4,6,7
 * 6	4:4,5,7
 * 7	4:4,5,6
 * 
 * Sample Output:
 * 
 * 1	1,2,3
 * 4	4,5,6,7
 * 
 */

package onramp.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CCFinalRound extends Configured implements Tool {

    public static final String NEIGHBOR_COMPID = "*";

	public static enum UpdateCounter {
		UPDATED;
	}

	public static class CCFinalRoundMapper extends Mapper<Text, Text, Text, Text> {

		private String compIdDelim;
		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();

			compIdDelim = conf.get("compIdDelim");
		}

		public void map(Text nodeId, Text value, Context context) throws IOException, InterruptedException {

			String valueStr = value.toString();


			ArrayList<String> tokens = new ArrayList<String>(Arrays.asList(valueStr.split(compIdDelim)));

	

			context.write(new Text(tokens.get(0)), new Text(tokens.get(1)));
		}
	}

	public static class CCFinalRoundReducer extends Reducer<Text, Text, Text, Text> {

		private String ajListDelim;
		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			ajListDelim = conf.get("ajListDelim");
		}

		public void reduce(Text nodeId, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String valueStr = null;
			String nodeListStr="";
			for (Text value : values) {
				
				valueStr = value.toString();
				
				
				for(String node: valueStr.split(ajListDelim)) {
					
					if(nodeListStr.equals("")) {
						nodeListStr=node;
					}else {
						if (!nodeListStr.contains(node)) {
						nodeListStr = nodeListStr + "," + node;
						}
					}
		
				}
				
			}


			context.write(nodeId, new Text(nodeListStr));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		String ajListDelim = args[2];
		String compIdDelim = args[3];

		Configuration conf;
		Job job;
		boolean runExitCode = false;
		

		conf = getConf();
	
		conf.set("ajListDelim", ajListDelim);
		conf.set("compIdDelim", compIdDelim);
		
		job = Job.getInstance(conf, "CCFinalRound");
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setJarByClass(CCFinalRound.class);
		
		job.setMapperClass(CCFinalRoundMapper.class);
		job.setReducerClass(CCFinalRoundReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		runExitCode = job.waitForCompletion(true);
		
		System.out.println("CCFinalRound Completed");


		return runExitCode ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int runExitCode = ToolRunner.run(new CCFinalRound(), args);
		System.exit(runExitCode);
	}
}


