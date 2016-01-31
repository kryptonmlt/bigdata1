import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigDataApplication {

	public static final String DATE_FROM = "DATE_FROM";

	public static final String DATE_TO = "DATE_TO";

	public static final String K = "K";

	public static final int RECORD_LENGTH = 14;

	public static final int RECORDS_PER_MAPPER = 3000;

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

		private Text reducerKey = new Text();
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			df.setTimeZone(TimeZone.getTimeZone("UTC"));

			// read query parameters
			Configuration conf = context.getConfiguration();
			Date dateFrom = null;
			Date dateTo = null;
			try {
				dateFrom = df.parse(conf.get(BigDataApplication.DATE_FROM)
						.replace("T", " ").replace("Z", " "));
				dateTo = df.parse(conf.get(BigDataApplication.DATE_TO)
						.replace("T", " ").replace("Z", " "));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			String k = conf.get(BigDataApplication.K);
			// System.out.println("Mapper Started!\nKEY: "+key+"\nValue: "+value.toString()+"\nMapper End!");

			StringTokenizer lines = new StringTokenizer(value.toString(),
					"\n\r\f");
			int recordCount = 0;
			Revision r = new Revision();

			// parse each line
			while (lines.hasMoreTokens()) {
				String line = lines.nextToken();
				StringTokenizer words = new StringTokenizer(line.toString());

				// par each word in line
				while (words.hasMoreTokens()) {
					String word = words.nextToken();
					if ("REVISION".equals(word)) {

						// process revision
						if (recordCount == 13) {
							// valid article.. all fields exist
							mapRevision(r, dateTo, dateFrom, context);

						} else if (recordCount != 0) {
							System.out.println("Invalid Article ..");
						}

						// start new revision
						recordCount = 0;
						r = new Revision();
						String tmpToken = "";
						// populate revision
						while (words.hasMoreTokens()
								&& !"CATEGORY".equals(tmpToken = words
										.nextToken())) {
							r.setRevisionFields(tmpToken.trim());
						}
					}
				}
				recordCount++;
			}
			mapRevision(r, dateTo, dateFrom, context);
		}

		public void mapRevision(Revision r, Date dateTo, Date dateFrom,
				Context context) throws IOException, InterruptedException {

			// validate revision.. all fields exist
			if (r.isValid()) {
				// check if revision occurred in
				// time interval
				if (r.getTimeStamp().before(dateTo)
						&& r.getTimeStamp().after(dateFrom)) {
					reducerKey.set(r.getArticleId());
					context.write(reducerKey, one);
				}
			} else {
				System.out.println("Invalid Revision ..");
			}
		}

		private class Revision {

			// article_id,revision_id,article title, timeStamp, ipUsername,
			// userId
			public String[] revisionField = new String[6];
			private int revCount = 0;

			public Revision() {

			}

			public void setRevisionFields(String value) {
				revisionField[revCount] = value;
				revCount++;
			}

			public String getArticleId() {
				return revisionField[0];
			}

			public String getRevisionId() {
				return revisionField[1];
			}

			public Date getTimeStamp() {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				df.setTimeZone(TimeZone.getTimeZone("UTC"));
				try {
					return df.parse(revisionField[3].replace("T", " ").replace(
							"Z", " "));
				} catch (ParseException e) {
					e.printStackTrace();
					return new Date();
				}
			}

			public boolean isValid() {
				return revCount == 6;
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

		System.out.println("BigDataApplication Started");
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf.pseudo/core-site.xml"));
		// int linesPerMapper = BigDataApplication.RECORD_LENGTH
		// * BigDataApplication.RECORDS_PER_MAPPER;
		// conf.setInt("mapreduce.input.lineinputformat.linespermap",linesPerMapper);

		String inputLoc = "/user/hadoop/wiki/wiki_1428.txt";
		String outputLoc = "/user/hadoop/wiki/output";

		Job job = null;
		switch (args.length) {
		case 2: // problem 1
			System.out.println("Prob 1");

			conf.set(BigDataApplication.DATE_FROM, args[0]);
			conf.set(BigDataApplication.DATE_TO, args[1]);

			job = Job.getInstance(conf, "BigData1");
			job.setJarByClass(BigDataApplication.class);
			job.setMapperClass(BigData1Mapper.class);
			job.setReducerClass(BigData1Reducer.class);
			job.setCombinerClass(BigData1Reducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			break;
		case 3: // problem 2
			System.out.println("Prob 2");

			conf.set(BigDataApplication.DATE_FROM, args[0]);
			conf.set(BigDataApplication.DATE_TO, args[1]);
			conf.set(BigDataApplication.K, args[2]);

			job = Job.getInstance(conf, "BigData2");
			job.setJarByClass(BigDataApplication.class);
			job.setMapperClass(BigData2Mapper.class);
			job.setReducerClass(BigData2Reducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			break;
		case 1: // problem 3
			System.out.println("Prob 3");

			conf.set(BigDataApplication.DATE_TO, args[0]);

			job = Job.getInstance(conf, "BigData3");
			job.setJarByClass(BigDataApplication.class);
			job.setMapperClass(BigData3Mapper.class);
			job.setReducerClass(BigData3Reducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			break;
		default:
			System.err.println("minimum 1 argument, maximum 3 arguments");
			System.exit(-1);
			break;
		}

		// job.setInputFormatClass(MultiLineInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputLoc));
		FileOutputFormat.setOutputPath(job, new Path(outputLoc));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
