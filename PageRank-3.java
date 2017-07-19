/***************************************************************************************************************************
 *Assignment:  Implement the PageRank Algorithm using MapReduce 
 *Student Name: Chandan Avdhut
 *  
 */

package onramp.hadoop;

import java.io.IOException;
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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;

 
public class PageRank extends Configured implements Tool {

    public static final double DAMPING_FACTOR=0.85;
    public static double EXP_CON=0.0000001;
    
    //Counter for convergence
	public static enum Counter {
		CON_DELTA;
	}

	public static class PageRankMapper extends Mapper<Text, Text, Text, Text> {

		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
		}

		public void map(Text nodeId, Text value, Context context) throws IOException, InterruptedException {

			//process first iteration
			if(conf.getInt("iteration", 0) == 1 ) {
				if ( !nodeId.toString().startsWith("#") ) {

			    int numNodes = conf.getInt("numNodes", 1);
			    double initialPageRank = 1.0 / (double) numNodes;
			    //initial probabilities
			    context.write(nodeId, new Text( Double.toString(initialPageRank)+   " ADJ_NODES:"+ value.toString()));
				}

				
			}else {
		
				String adjNodes ="";
				String valueStr = value.toString();
				String tokens[]=valueStr.split(" ADJ_NODES:");
				double rank = Double.parseDouble(tokens[0]);
				if (tokens.length > 1) {
					String adjNodeArray[] = tokens[1].split(",");
					for(String adjNode:adjNodeArray){
						if (adjNodes.equals("")) {
							adjNodes=adjNode;
						} else {
							adjNodes+= "," + adjNode;
						}
						//distribute probabalities 
						context.write(new Text(adjNode), new Text( Double.toString(rank/adjNodeArray.length)));	
					}
				}
				
			
			context.write(nodeId, value);
				}
		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
		}

		public void reduce(Text nodeId, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String valueStr = null;

			double prerank=0;
			String adjNodes="";
			double rank=0;
			if (conf.getInt("iteration", 0) == 1 ) {				
				for (Text value : values) {
					valueStr = value.toString();
					String tokens[]=valueStr.split(" ADJ_NODES:");
					if (tokens.length > 1) {
						rank=Double.parseDouble(tokens[0]);
						if(adjNodes.equals("")) 
							adjNodes=tokens[1];
						else
							adjNodes+= ","+tokens[1];
					}else {
						rank=Double.parseDouble(tokens[0]);
					}
				}
				//write initial probablities and adj nodes 
				context.write(nodeId, new Text( rank + " ADJ_NODES:"+  adjNodes));
				
			}else  {
			for (Text value : values) {
				valueStr = value.toString();
				String tokens[]=valueStr.split(" ADJ_NODES:");
				if (tokens.length > 1) {
					prerank= Double.parseDouble(tokens[0]);
					adjNodes=tokens[1];
				}else {
					rank+=Double.parseDouble(tokens[0]);
				}
			}
			

			//Damping factor 
			rank = ((1.0 - DAMPING_FACTOR) / (double) conf.getInt("numNodes", 1)) + (DAMPING_FACTOR * rank);
			
			double delta = prerank - rank;

			if (delta > EXP_CON) {
				//convergence  counter
				context.getCounter(Counter.CON_DELTA).increment(1);
			}
			context.write(nodeId, new Text( rank + " ADJ_NODES:"+  adjNodes));
		}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		int numNodes=getNumNodes(in);
		System.out.println("numNodes=" + numNodes);
		Configuration conf;
		Job job;
		boolean runExitCode = false;
		Path iteration_out_path = in;
		int iteration = 0;
		
		do {

			iteration++;

			conf = getConf();
			in = iteration_out_path;
			iteration_out_path = new Path(out, "PR-" + Integer.toString(iteration));
			conf.setInt("iteration", iteration);
			conf.setInt("numNodes", numNodes);

			job = Job.getInstance(conf, "PageRank_Iteration_" + Integer.toString(iteration));

			FileInputFormat.setInputPaths(job, in);
			FileOutputFormat.setOutputPath(job, iteration_out_path);

			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			runExitCode = job.waitForCompletion(true);
		    if (iteration > 1 ) {
				long convergence = job.getCounters().findCounter(Counter.CON_DELTA).getValue();
			   System.out.print("Covergence for iteration " + iteration + " = " + convergence);
				if (convergence< 1) {
					System.out.println("Done!!");
					break;
				}
		    }
			//if(iteration > 20 ) {
			//	break;
			//}
			
		} while (true);

		return runExitCode ? 0 : 1;
	}
	//Get number of nodes in input file
	  public static int getNumNodes(Path file)
		      throws IOException {
		    Configuration conf = new Configuration();		    
		    FileSystem fs = file.getFileSystem(conf);
		    LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
		    Set<String> nodes = new HashSet<String>();
		    
		    while (iter.hasNext()) {
		      String line = iter.nextLine();
		      if(!line.startsWith("#") ) {

		      String[] parts = StringUtils.split(line);
		      nodes.add(parts[0]);
		      nodes.add(parts[1]);
		      }
		     
		    }
		    return nodes.size();
		  }

	public static void main(String[] args) throws Exception {
		int runExitCode = ToolRunner.run(new PageRank(), args);
		System.exit(runExitCode);
	}
}
