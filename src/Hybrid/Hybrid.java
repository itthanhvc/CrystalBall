package Hybrid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import PartitionHelper.PartitionHelper;

public class Hybrid extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Configuration conf = new Configuration();
		// Get key as CustomerName as first Key to split inputs
		conf.set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				"\t ");
		int res = ToolRunner.run(conf, new Hybrid(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Hybrid");
		
		job.setJarByClass(Hybrid.class);
		job.setOutputKeyClass(Neighbor.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setPartitionerClass(PartitionHelper.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);	 

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(PartitionHelper.getReducerNumber());
	
		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends Mapper<Text, Text, Neighbor, IntWritable> {
		private HashMap<Neighbor, Integer> neighborHash;
		// Stored Unique Key for restoring
		private static TreeSet<Neighbor> uniqueOrder;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			neighborHash = new HashMap<Neighbor, Integer>();
			uniqueOrder = new TreeSet<Neighbor>();
		}

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] contents = value.toString().trim().split("\\s+");
			for (int i = 0; i < contents.length; i++) {
				for (int j = i + 1; j < contents.length
						&& !contents[i].equals(contents[j]); j++) {
					Neighbor test = new Neighbor(contents[i], contents[j]);
					if (!neighborHash.containsKey(test)) {
						neighborHash.put(test, 1);
						uniqueOrder.add(test);
					} else {
						neighborHash.put(test, neighborHash.get(test) + 1);
					}
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws java.io.IOException,
				java.lang.InterruptedException {
			Iterator<Neighbor> key = uniqueOrder.iterator();
			while (key.hasNext()) {
				Neighbor pair = key.next();
				context.write(pair, new IntWritable(neighborHash.get(pair)));
			}

		}
	}

	public static class Reduce extends
			Reducer<Neighbor, IntWritable, Text, TextArrayWritable> {
		private static int marginal = 0;
		private static String currentTerm = null;
		// Stored Unique Key for restoring
		private static TreeSet<String> uniqueOrder = new TreeSet<String>();
		// Store actual map
		private static HashMap<String, Double> stripArray = new HashMap<String, Double>();

		@Override
		public void reduce(Neighbor key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			if (currentTerm == null) {
				currentTerm = key.getPrimary().toString();
			}
			if (!currentTerm.equals(key.getPrimary().toString())) {
				List<String> convert = new ArrayList<String>();
				Iterator<String> stripKey = uniqueOrder.iterator();
				while (stripKey.hasNext()) {
					String term = stripKey.next();
					Double value = stripArray.get(term);
					// stripArray.put(term, new Double(value / marginal));
					convert.add(String.format("(%s:%f)", term,
							value / marginal ));
				}
				
				context.write(
						new Text(currentTerm),
						new TextArrayWritable(convert
								.toArray(new String[convert.size()])));
				marginal = 0;
				stripArray.clear();
				uniqueOrder.clear();
				currentTerm = key.getPrimary().toString();
			}
			for (IntWritable count : values) {
				if (!stripArray.containsKey(key.getForeign().toString())) {
					uniqueOrder.add(key.getForeign().toString());
					stripArray.put(key.getForeign().toString(),
							count.get() * 1.0);
					marginal += count.get();
					continue;
				}
				Double value = stripArray.get(key.getForeign().toString())
						+ count.get();
				stripArray.put(key.getForeign().toString(), value);
				marginal += count.get();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			List<String> convert = new ArrayList<String>();
			Iterator<String> stripKey = uniqueOrder.iterator();
			
			while (stripKey.hasNext()) {
				String term = stripKey.next();
				 Double value = stripArray.get(term);
				// stripArray.put(term, new Double(value / marginal));
				convert.add(String.format("(%s:%f)", term,
						value / marginal));
			}

			context.write(
					new Text(currentTerm),
					new TextArrayWritable(convert.toArray(new String[convert
							.size()])));
		}
	}
}