

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Pairs extends Configured implements Tool {
	
	private static class PairsMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

		private final TextPair pair = new TextPair();
		private final IntWritable one = new IntWritable(1);
		private int window = 2;

		public void setup(Context context) {
			window = context.getConfiguration().getInt("window", 2);
		}

		public void map(LongWritable key, Text line, Context context) throws IOException,
				InterruptedException {
			String text = line.toString();

			//String[] terms = text.split("\\s+");
			//String[] terms= text.split("\\s+\\pP|\\pS");
			//String[] terms= text.split("\\s+[\\w+]");
			String regex="[^0-9](\\$)|(\\{.+?\\})\\pP|\\pS|\\s+|\t|\r|\n|\\,|\\*|\\?|\\:|\\!|\\||\\;|\\,|\\.\042|\\$[\\[\\]]/\"/g,";
			String[] terms= text.split(regex);
			//String[] terms= text.split("\\pP|\\pS|\\s+|\t|\r|\n|\\,|\\*|\\?|\\!|\\|\\-|\\.|\\;[\\[\\]]");
			//===============================================
			
			//=========================================================
			

			for (int i = 0; i < terms.length; i++) {
				String term = terms[i];

				// jump over empty token
				if (term.length() == 0)
					continue;

				for (int j = i - window; j < i + window + 1; j++) {
					if (j == i || j < 0)
						continue;

					if (j >= terms.length)
						break;

					// jump over empty token
					if (terms[j].length() == 0)
						continue;

					pair.set(term, terms[j]);
					context.write(pair, one);
				}
			}
		}
	}

	private static class PairsReducer extends
			Reducer<TextPair, IntWritable, TextPair, IntWritable> {

		private final static IntWritable SumOfValue = new IntWritable();

		public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}

			SumOfValue.set(sum);
			context.write(key, SumOfValue);
		}
	}

	
	public Pairs() {
	}

	public void run(String[] args) throws Exception {

		String inputPath = args[0];
		String outputPath = args[1];

		int window = Integer.parseInt(args[2]);

		Job job = new Job(getConf(), "Word Co occurrence Matrix");

		Path outputDir = new Path(outputPath);
		FileSystem.get(getConf()).delete(outputDir, true);

		job.getConfiguration().setInt("window", window);

		job.setJarByClass(Pairs.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(PairsMapper.class);
		job.setCombinerClass(PairsReducer.class);
		job.setReducerClass(PairsReducer.class);

		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Pairs(), args);
		System.exit(res);
	}
}