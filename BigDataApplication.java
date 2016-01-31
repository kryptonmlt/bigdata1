import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigDataApplication {

	public static final String DATE_FROM = "DATE_FROM";

	public static final String DATE_TO = "DATE_TO";

	public static final String K = "K";

	public static final int RECORD_LENGTH = 14;

	public static final int RECORDS_PER_MAPPER = 1000;

	public static class BigData1Mapper extends
			Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) {
			Configuration conf = context.getConfiguration();
			String dateFrom = conf.get(BigDataApplication.DATE_FROM);
			String dateTo = conf.get(BigDataApplication.DATE_TO);
		}

	}

	public static class BigData1Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

		}
	}

	public static class BigData2Mapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String dateFrom = conf.get(BigDataApplication.DATE_FROM);
			String dateTo = conf.get(BigDataApplication.DATE_TO);
			String k = conf.get(BigDataApplication.K);

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				word.set(token);
				context.write(word, one);
			}
		}

	}

	public static class BigData2Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class BigData3Mapper extends
			Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) {
			Configuration conf = context.getConfiguration();
			String dateTo = conf.get(BigDataApplication.DATE_TO);
		}

	}

	public static class BigData3Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf.pseudo/core-site.xml"));
		conf.setInt(NLineInputFormat.NUM_INPUT_FILES,
				BigDataApplication.RECORD_LENGTH
						* BigDataApplication.RECORDS_PER_MAPPER);

		String inputLoc = "/user/hadoop/wiki/wiki_1428.txt";
		String outputLoc = "/user/hadoop/wiki/output";

		Job job = null;
		switch (args.length) {
		case 2: // problem 1
			job = Job.getInstance(conf, "BigData1");
			job.setJarByClass(BigDataApplication.class);
			job.setMapperClass(BigData1Mapper.class);
			job.setReducerClass(BigData1Reducer.class);

			conf.set(BigDataApplication.DATE_FROM, args[0]);
			conf.set(BigDataApplication.DATE_TO, args[1]);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			break;
		case 3: // problem 2
			job = Job.getInstance(conf, "BigData2");
			job.setJarByClass(BigDataApplication.class);
			job.setMapperClass(BigData2Mapper.class);
			job.setReducerClass(BigData2Reducer.class);

			conf.set(BigDataApplication.DATE_FROM, args[0]);
			conf.set(BigDataApplication.DATE_TO, args[1]);
			conf.set(BigDataApplication.K, args[2]);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			break;
		case 1: // problem 3
			job = Job.getInstance(conf, "BigData3");
			job.setJarByClass(BigDataApplication.class);
			job.setMapperClass(BigData3Mapper.class);
			job.setReducerClass(BigData3Reducer.class);
			conf.set(BigDataApplication.DATE_TO, args[0]);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			break;
		default:
			System.err.println("minimum 1 argument, maximum 3 arguments");
			System.exit(-1);
			break;

		}

		job.setInputFormatClass(NLineInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputLoc));
		FileOutputFormat.setOutputPath(job, new Path(outputLoc));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
