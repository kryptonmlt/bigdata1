import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigDataApplication {

	public static final String DATE_FROM = "DATE_FROM";

	public static final String DATE_TO = "DATE_TO";

	public static final String K = "K";

	/**
	 * Gathers revision information from records for task1
	 * 
	 * @author kurtp
	 */
	public static class Task1Mapper extends
			Mapper<Object, Text, LongWritable, LongWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// read query parameters
			Configuration conf = context.getConfiguration();
			Date dateFrom = null;
			Date dateTo = null;
			try {
				dateFrom = Tools.getDateFromWikiString(conf
						.get(BigDataApplication.DATE_FROM));
				dateTo = Tools.getDateFromWikiString(conf
						.get(BigDataApplication.DATE_TO));
			} catch (ParseException e) {
				e.printStackTrace();
			}

			StringTokenizer lines = new StringTokenizer(value.toString(),
					"\n\r\f");
			while (lines.hasMoreTokens()) {
				String line = lines.nextToken();
				String[] words = line.split(" ");

				// par each word in line
				if ("REVISION".equals(words[0])) {

					// populate revision
					String articleId = words[1];
					String revisionId = words[2];
					String timeStamp = words[4];

					Date timeStampDate;
					try {
						timeStampDate = Tools.getDateFromWikiString(timeStamp);
					} catch (ParseException e) {
						e.printStackTrace();
						timeStampDate = new Date();
					}

					if (timeStampDate.before(dateTo)
							&& timeStampDate.after(dateFrom)) {
						context.write(
								new LongWritable(Long.parseLong(articleId)),
								new LongWritable(Long.parseLong(revisionId)));
					}

				}
			}
		}

	}

	/**
	 * Receives Articles with different revisions and timestamps. Sorts the
	 * revisions in this time interval
	 * 
	 * @author kurtp
	 *
	 */
	public static class Task1Reducer extends
			Reducer<LongWritable, LongWritable, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			List<Long> revisions = new ArrayList<Long>();
			for (LongWritable value : values) {
				revisions.add(value.get());
			}
			Collections.sort(revisions);
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < revisions.size(); i++) {
				if (i == revisions.size()) {
					sb.append(revisions.get(i));
				} else {
					sb.append(revisions.get(i)).append(" ");
				}
			}
			context.write(key, new Text(revisions.size() + " " + sb.toString()));
		}
	}

	/**
	 * Gathers revision information from records for task1
	 * 
	 * @author kurtp
	 */
	public static class Task3Mapper extends
			Mapper<Object, Text, LongWritable, RevisionTimeStampWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// read query parameters
			Configuration conf = context.getConfiguration();
			Date dateTo = null;
			try {
				dateTo = Tools.getDateFromWikiString(conf
						.get(BigDataApplication.DATE_TO));
			} catch (ParseException e) {
				e.printStackTrace();
			}

			StringTokenizer lines = new StringTokenizer(value.toString(),
					"\n\r\f");
			while (lines.hasMoreTokens()) {
				String line = lines.nextToken();
				String[] words = line.split(" ");

				// par each word in line
				if ("REVISION".equals(words[0])) {

					// populate revision
					String articleId = words[1];
					String revisionId = words[2];
					String timeStamp = words[4];

					Date timeStampDate;
					try {
						timeStampDate = Tools.getDateFromWikiString(timeStamp);
					} catch (ParseException e) {
						e.printStackTrace();
						timeStampDate = new Date();
					}

					if (timeStampDate.before(dateTo)) {
						context.write(
								new LongWritable(Long.parseLong(articleId)),
								new RevisionTimeStampWritable(Long
										.parseLong(revisionId), timeStamp));
					}

				}
			}
		}

	}

	/**
	 * Receives Articles with different revisions and timestamps. Sorts the
	 * revisions in this time interval
	 * 
	 * @author kurtp
	 *
	 */
	public static class Task3Reducer
			extends
			Reducer<LongWritable, RevisionTimeStampWritable, LongWritable, Text> {

		public void reduce(LongWritable key,
				Iterable<RevisionTimeStampWritable> values, Context context)
				throws IOException, InterruptedException {

			Date latestDate = null;
			Long revisionId = null;
			String timeStampResult = "";
			try {
				for (RevisionTimeStampWritable value : values) {
					Date revTime = Tools.getDateFromWikiString(value
							.getTimeStamp());
					if (latestDate == null) {
						latestDate = revTime;
						timeStampResult = value.getTimeStamp();
						revisionId = value.getRevisionId();
					} else if (revTime.after(latestDate)) {
						latestDate = revTime;
						timeStampResult = value.getTimeStamp();
						revisionId = value.getRevisionId();
					}
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
			context.write(key, new Text(revisionId + " " + timeStampResult));
		}
	}

	/**
	 * Gathers revision information from records
	 * 
	 * @author kurtp
	 */
	public static class Task2Mapper extends
			Mapper<Object, Text, LongWritable, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// read query parameters
			Configuration conf = context.getConfiguration();
			Date dateFrom = null;
			Date dateTo = null;
			try {
				dateFrom = Tools.getDateFromWikiString(conf
						.get(BigDataApplication.DATE_FROM));
				dateTo = Tools.getDateFromWikiString(conf
						.get(BigDataApplication.DATE_TO));
			} catch (ParseException e) {
				e.printStackTrace();
			}

			StringTokenizer lines = new StringTokenizer(value.toString(),
					"\n\r\f");
			while (lines.hasMoreTokens()) {
				String line = lines.nextToken();
				String[] words = line.split(" ");

				// par each word in line
				if ("REVISION".equals(words[0])) {

					// populate revision
					String articleId = words[1];
					String timeStamp = words[4];

					Date timeStampDate;
					try {
						timeStampDate = Tools.getDateFromWikiString(timeStamp);
					} catch (ParseException e) {
						e.printStackTrace();
						timeStampDate = new Date();
					}

					if (timeStampDate.before(dateTo)
							&& timeStampDate.after(dateFrom)) {
						context.write(
								new LongWritable(Long.parseLong(articleId)),
								new IntWritable(1));
					}

				}
			}
		}
	}

	/**
	 * Receives Articles with different revisions and timestamps. Adds them up
	 * together.
	 * 
	 * @author kurtp
	 *
	 */
	public static class Task2Combiner extends
			Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

		public void reduce(LongWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * Receives Articles with different revisions and timestamps. Adds up the
	 * total number of modifications
	 * 
	 * @author kurtp
	 *
	 */
	public static class Task2Reducer extends
			Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

		public void reduce(LongWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * Extracts the modifications and articles to send them to the reducers for
	 * sorting keeping only the highest K
	 * 
	 * @author cloudera
	 *
	 */
	public static class Task2SortMapper extends
			Mapper<Object, Text, NullWritable, Text> {

		// Our output key and value Writables
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			long k = Long.parseLong(conf.get(BigDataApplication.K));

			StringTokenizer words = new StringTokenizer(value.toString());

			// par each word in line
			while (words.hasMoreTokens()) {
				String articleId = words.nextToken();
				String modifications = words.nextToken();

				repToRecordMap.put(Integer.parseInt(modifications), new Text(
						articleId + " " + modifications));

				if (repToRecordMap.size() > k) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	/**
	 * Sorts articles according to the number of modifications keeping only the
	 * highest K
	 * 
	 * @author cloudera
	 *
	 */
	public static class Task2SortReducer extends
			Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			long k = Long.parseLong(conf.get(BigDataApplication.K));

			for (Text value : values) {

				StringTokenizer words = new StringTokenizer(value.toString());
				String articleId = words.nextToken();
				String modifications = words.nextToken();

				repToRecordMap.put(Integer.parseInt(modifications), new Text(
						articleId + " " + modifications));

				if (repToRecordMap.size() > k) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

			for (Text t : repToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	/**
	 * Random Tools
	 * 
	 * @author kurtp
	 *
	 */
	public static class Tools {

		private Tools() {

		}

		/**
		 * Convert WikiString to Date
		 *
		 */
		public static Date getDateFromWikiString(String wikiText)
				throws ParseException {
			if (wikiText == null) {
				return null;
			}
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			df.setTimeZone(TimeZone.getTimeZone("UTC"));
			return df.parse(wikiText.replace("T", " ").replace("Z", ""));
		}
	}

	/**
	 * Output Mapper object containing revision Id and TimeStamp
	 * 
	 * @author kurtp
	 *
	 */
	public static class RevisionTimeStampWritable implements Writable,
			WritableComparable<RevisionTimeStampWritable> {

		private Long revisionId;
		private String timeStamp;

		public RevisionTimeStampWritable() {
		}

		public RevisionTimeStampWritable(long revisionId, String timeStamp) {
			this.revisionId = revisionId;
			this.timeStamp = timeStamp;
		}

		@Override
		public String toString() {
			return (new StringBuilder().append(revisionId)).append(" ")
					.append(timeStamp).toString();
		}

		public void readFields(DataInput dataInput) throws IOException {
			revisionId = WritableUtils.readVLong(dataInput);
			timeStamp = WritableUtils.readString(dataInput);
		}

		public void write(DataOutput dataOutput) throws IOException {
			WritableUtils.writeVLong(dataOutput, revisionId);
			WritableUtils.writeString(dataOutput, timeStamp);
		}

		public int compareTo(RevisionTimeStampWritable objKeyPair) {
			int result = revisionId.compareTo(objKeyPair.revisionId);
			return result;
		}

		public Long getRevisionId() {
			return revisionId;
		}

		public void setRevisionId(Long revisionId) {
			this.revisionId = revisionId;
		}

		public String getTimeStamp() {
			return timeStamp;
		}

		public void setTimeStamp(String timeStamp) {
			this.timeStamp = timeStamp;
		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println("BigDataApplication Started");
		Configuration conf = new Configuration();

		// assignment final conf
		// String hdfs_home = "/user/2222148p/";
		// conf.addResource(new Path("bd4-hadoop/conf/core-site.xml"));
		// conf.set("mapred.jar", "/users/msc/2222148p/KurtJimmiBD.jar");
		// String inputLoc = "/user/bd4-ae1/enwiki-20080103-full.txt";
		// String outputLoc = hdfs_home + "output";
		// String tempLoc = hdfs_home + "temp";

		/*
		 * conf.setBoolean("mapred.compress.map.output", true);
		 * conf.setClass("mapred.map.output.compression.codec", GzipCodec.class,
		 * CompressionCodec.class);
		 */

		// localhost stuff
		conf.addResource(new Path("/etc/hadoop/conf.pseudo/core-site.xml"));
		String hdfs_home = "/user/hadoop/wiki/";
		String inputLoc = hdfs_home + "wiki_1428.txt";
		String outputLoc = hdfs_home + "output/";
		String tempLoc = hdfs_home + "temp/";

		Job job = null;
		switch (args.length) {
		case 2: // problem 1
			System.out.println("Task 1");

			conf.set(BigDataApplication.DATE_FROM, args[0]);
			conf.set(BigDataApplication.DATE_TO, args[1]);

			job = Job.getInstance(conf, "BigData1");

			job.setMapperClass(Task1Mapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(LongWritable.class);

			job.setReducerClass(Task1Reducer.class);
			job.setOutputValueClass(Text.class);
			break;
		case 3: // problem 2
			System.out.println("Task 2");

			conf.set(BigDataApplication.DATE_FROM, args[0]);
			conf.set(BigDataApplication.DATE_TO, args[1]);
			conf.set(BigDataApplication.K, args[2]);

			job = Job.getInstance(conf, "BigData2");

			job.setMapperClass(Task2Mapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setReducerClass(Task2Reducer.class);
			job.setCombinerClass(Task2Combiner.class);
			job.setOutputValueClass(IntWritable.class);

			// job.setPartitionerClass(ArticleIdModificationsPartitioner.class);
			// job.setGroupingComparatorClass(ArticleIdModificationsGroupingComparator.class);

			break;
		case 1: // problem 3
			System.out.println("Task 3");

			conf.set(BigDataApplication.DATE_TO, args[0]);

			job = Job.getInstance(conf, "BigData3");

			job.setMapperClass(Task3Mapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(RevisionTimeStampWritable.class);

			job.setReducerClass(Task3Reducer.class);
			job.setOutputValueClass(Text.class);
			break;
		default:
			System.err.println("minimum 1 argument, maximum 3 arguments");
			System.exit(-1);
			break;
		}

		job.setJarByClass(BigDataApplication.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);

		FileSystem hdfs = FileSystem.get(conf);
		// delete output directory before using it
		hdfs.delete(new Path(outputLoc), true);

		FileInputFormat.addInputPath(job, new Path(inputLoc));
		if (args.length == 3) {
			// delete temp directory before using it
			hdfs.delete(new Path(tempLoc), true);
			FileOutputFormat.setOutputPath(job, new Path(tempLoc));
		} else {
			FileOutputFormat.setOutputPath(job, new Path(outputLoc));
		}
		long timeStarted = System.currentTimeMillis();
		// waiting for job to finish
		boolean job1FinishedCorrectly = job.waitForCompletion(true);

		System.out.println("Job Execution time in ms: "
				+ (System.currentTimeMillis() - timeStarted));

		if (job1FinishedCorrectly) {

			hdfs.delete(new Path(hdfs_home + "sortedResult"), true);
			// check if it is problem 2
			if (args.length == 3) {
				// start job that does the sorting
				Job job2 = Job.getInstance(conf, "BigData2Sorting");
				job2.setReducerClass(Task2SortReducer.class);
				job2.setJarByClass(BigDataApplication.class);
				job2.setMapperClass(Task2SortMapper.class);
				job2.setMapOutputKeyClass(NullWritable.class);
				job2.setMapOutputValueClass(Text.class);
				job2.setOutputKeyClass(NullWritable.class);
				job2.setOutputValueClass(Text.class);
				job2.setInputFormatClass(TextInputFormat.class);
				job2.setNumReduceTasks(1);

				FileInputFormat.addInputPath(job2, new Path(tempLoc));
				FileOutputFormat.setOutputPath(job2, new Path(outputLoc));
				long time2ndStarted = System.currentTimeMillis();

				// wait for sorting to finish
				int result = job2.waitForCompletion(true) ? 0 : 1;
				System.out.println("Sorting Job Execution time in ms: "
						+ (System.currentTimeMillis() - time2ndStarted));

				FileUtil.copyMerge(hdfs, new Path(outputLoc), hdfs, new Path(
						hdfs_home + "sortedResult"), false, conf, null);
				System.exit(result);
			} else {
				// merge outputs
				FileUtil.copyMerge(hdfs, new Path(outputLoc), hdfs, new Path(
						hdfs_home + "sortedResult"), false, conf, null);
			}
		}
		System.exit(job1FinishedCorrectly ? 0 : 1);
	}
}
