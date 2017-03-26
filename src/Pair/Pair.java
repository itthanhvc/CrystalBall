package Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import PartitionHelper.PartitionHelper;

public class Pair extends Configured implements Tool {

	public static class myPair extends
			Mapper<LongWritable, Text, MultipleKey, IntWritable> {

		List<String> arraylist = new ArrayList<>();
		Map<MultipleKey, Integer> clean = new HashMap<>();
		MultipleKey list;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String val[];
			arraylist.clear();

			val = value.toString().split("\t");

			for (String t : val[1].split(" ")) {
				arraylist.add(t.trim());
			}

			for (int i = 0; i < arraylist.size(); i++) {
				for (int j = i + 1; j < arraylist.size()
						&& !arraylist.get(i).isEmpty()
						&& !arraylist.get(i).equals(arraylist.get(j)); j++) {

					list = new MultipleKey(arraylist.get(i), "*");

					if (!clean.containsKey(list)) {
						clean.put(list, 1);
						clean.put(
								new MultipleKey(arraylist.get(i), arraylist
										.get(j)), 1);
					} else {
						clean.put(list, clean.get(list) + 1);
						clean.put(
								new MultipleKey(arraylist.get(i), arraylist
										.get(j)), clean.get(list) + 1);
					}
				}
			}
		}

		@Override
		public void cleanup(Context context) throws java.io.IOException,
				java.lang.InterruptedException {

			for (Map.Entry<MultipleKey, Integer> key : clean.entrySet()) {
				context.write(key.getKey(), new IntWritable(key.getValue()));
			}
		}

	}

	public static class Reduce extends
			Reducer<MultipleKey, IntWritable, Text, Text> {

		double marginal = 0, sum = 0;

		@Override
		public void reduce(MultipleKey key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			if (key.getValue().equals("*")) {
				marginal = 0;
				for (IntWritable k : values) {
					marginal += k.get();
				}
			} else {
				sum = 0;

				for (IntWritable k : values) {
					sum += k.get();
				}

				context.write(new Text(key.toString()),
						new Text(Double.toString(sum / marginal)));
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Pair");
		job.setJarByClass(Pair.class);
		job.setOutputKeyClass(MultipleKey.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(myPair.class);
		job.setPartitionerClass(PartitionHelper.class);
		job.setReducerClass(Reduce.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(PartitionHelper.getReducerNumber());
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new Pair(), args);
		System.exit(res);
	}
}