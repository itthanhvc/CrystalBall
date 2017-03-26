package Stripes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

class TWritable extends ArrayWritable {

	public TWritable() {

		super(Text.class);

	}

	public TWritable(String[] strings) {

		super(Text.class);
		Text[] texts = new Text[strings.length];
		for (int i = 0; i < strings.length; i++) {
			texts[i] = new Text(strings[i]);
		}
		set(texts);
	}

	@Override
	public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(", {");
		for (int i = 0; i < super.toStrings().length; i++) {
			if (i != super.toStrings().length - 1)
				sb.append(super.toStrings()[i]).append(", ");
			else
				sb.append(super.toStrings()[i]);
		}
		sb.append("}");
		return sb.toString();
	}
}

public class Stripes extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Configuration conf = new Configuration();

		conf.set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				"\t ");

		int res = ToolRunner.run(conf, new Stripes(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Stripes");
		job.setJarByClass(Stripes.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(TWritable.class);

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

	public static class Map extends Mapper<Text, Text, Text, MapWritable> {
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().trim().split("\\s+");
			if (tokens.length > 1) {
				for (int i = 0; i < tokens.length - 1; i++) {
					MapWritable H = new MapWritable();
					for (int j = i + 1; j < tokens.length
							&& !tokens[i].equals(tokens[j]); j++) {
						
						if (H.containsKey(new Text(tokens[j]))) {
							int v = Integer.parseInt(H.get(new Text(tokens[j]))
									.toString());
							H.put(new Text(tokens[j]), new IntWritable(v + 1));
						} else
							H.put(new Text(tokens[j]), new IntWritable(1));
					}
					context.write(new Text(tokens[i]), H);
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, MapWritable, Text, TWritable> {

		private static String currentTerm = null;
		private static int totalmarginal = 0;
		private static TreeSet<String> order = new TreeSet<String>();
		private static HashMap<String, Double> array = new HashMap<>();

		@Override
		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {
			
			if (currentTerm == null) {
				currentTerm = key.toString();
			}

			if (!currentTerm.equals(key.toString())) {

				ArrayList<String> cvt = new ArrayList<>();
				Iterator<String> ikey = order.iterator();
				while (ikey.hasNext()) {
					String term = ikey.next();
					Double value = array.get(term);
					
					cvt.add(String.format("(%s:%f)", term, value
							/ totalmarginal));
					
				}
				
				context.write(new Text(currentTerm),
						new TWritable(cvt.toArray(new String[cvt.size()])));
				
				
				totalmarginal = 0;
				array.clear();
				order.clear();
				currentTerm = key.toString();

			}

			Iterator<MapWritable> it = values.iterator();
			while (it.hasNext()) {
				MapWritable m = it.next();
				Iterator<Entry<Writable, Writable>> itcount = m.entrySet()
						.iterator();

				while (itcount.hasNext()) {
					Entry<Writable, Writable> entry = itcount.next();
//					Double count = Integer
//							.parseInt(entry.getValue().toString()) * 1.0;
					int count = Integer
							.parseInt(entry.getValue().toString());
					
					if (!array.containsKey(entry.getKey().toString())) {
						order.add(entry.getKey().toString());
						array.put(entry.getKey().toString(), count*1.0);

						totalmarginal += count;
						continue;
					}

					Double value = array.get(entry.getKey().toString()) + count;
					array.put(entry.getKey().toString(), value);
					totalmarginal += count;
				}
			}
			

		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {

			ArrayList<String> cvt = new ArrayList<String>();
			Iterator<String> ikey = order.iterator();

			while (ikey.hasNext()) {
				String term = ikey.next();
				Double value = array.get(term);

				cvt.add(String.format("(%s:%f)", term, value / totalmarginal));

			}

			context.write(new Text(currentTerm),
					new TWritable(cvt.toArray(new String[cvt.size()])));
		}
	}
}