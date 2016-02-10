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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigDataApplication {

	public static final String DATE_FROM = "DATE_FROM";

	public static final String DATE_TO = "DATE_TO";

	public static final String K = "K";

	public static final int MAX_ARTICLE_ID = 15071261;

	public static final long MIN_ARTICLE_ID = 6;

	/**
	 * Gathers article ids and sends them to the reducer
	 * 
	 * @author kurtp
	 */
	public static class MaxArticleIdMapper extends
			Mapper<Object, Text, NullWritable, LongWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer lines = new StringTokenizer(value.toString(),
					"\n\r\f");
			while (lines.hasMoreTokens()) {
				String line = lines.nextToken();
				String[] words = line.split(" ");

				// par each word in line
				if ("REVISION".equals(words[0])) {

					// populate revision
					String articleId = words[1];

					context.write(NullWritable.get(),
							new LongWritable(Long.parseLong(articleId)));

				}
			}
		}

	}

	/**
	 * Receives Articles Ids and outputs the maximum and minimum
	 * 
	 * @author kurtp
	 *
	 */
	public static class MaxArticleIdReducer extends
			Reducer<NullWritable, LongWritable, LongWritable, NullWritable> {

		public void reduce(NullWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			Long maxId = Long.MIN_VALUE;
			Long minId = Long.MAX_VALUE;
			for (LongWritable value : values) {
				if (value.get() > maxId) {
					maxId = value.get();
				}
				if (value.get() < minId) {
					minId = value.get();
				}
			}
			context.write(new LongWritable(maxId), NullWritable.get());
			context.write(new LongWritable(minId), NullWritable.get());
		}
	}

	/**
	 * outputs article id, revision id pair the data the revision is posted must
	 * be in between DATE_FROM and DATE_TO
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
	 * Receives an article id with its associated revisions and then prints them
	 * out.
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
	 * outputs article id, RevisionTimeStampWritable RevisionTimeStampWritable
	 * contains revision id and timestamp the data the revision is posted must
	 * be before DATE_TO
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
	 * Receives an article id with its associated revisions and timestamps. Then
	 * prints out the nearest date to today possible. (the Mapper would have
	 * already removed revisions posted after DATE_TO)
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
	 * outputs <article id, 1 > 1 represents the number of revisions revision
	 * must be in between DATE_FROM and DATE_TO
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
	 * Receives Article Ids as Keys. number of modifications inside the values.
	 * Adds up the total number of modifications and then writes them for each
	 * article id.
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
	 * @author kurtp
	 *
	 */
	public static class Task2SortMapper extends
			Mapper<Object, Text, ArticleIdModificationsWritable, NullWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer words = new StringTokenizer(value.toString());

			// par each word in line
			while (words.hasMoreTokens()) {
				String articleId = words.nextToken();
				String modifications = words.nextToken();

				context.write(
						new ArticleIdModificationsWritable(Long
								.parseLong(articleId), Integer
								.parseInt(modifications)), NullWritable.get());
			}
		}
	}

	/**
	 * Sorts articles according to the number of modifications keeping only the
	 * highest K All articles id arrive in 1 reducer thus the first K are chosen
	 * in this method
	 * 
	 * @author kurtp
	 *
	 */
	public static class Task2SortReducer
			extends
			Reducer<ArticleIdModificationsWritable, NullWritable, LongWritable, IntWritable> {

		public void reduce(ArticleIdModificationsWritable key,
				Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			int k = Integer.parseInt(conf.get(BigDataApplication.K));
			//no race conditions since 1 reducer
			if (k > 0) {//print articles as long as k is not 0
				context.write(new LongWritable(key.getArticleId()),
						new IntWritable(key.getModifications()));
				k--;
			} else {
				// stop reducer
				cleanup(context);
			}
			conf.setInt(BigDataApplication.K, k);
		}
	}

	/**
	 * Partitions article ids to the appropriate reducer
	 * 
	 * @author kurtp
	 *
	 */
	public static class Task1Partitioner extends
			Partitioner<LongWritable, LongWritable> {

		@Override
		public int getPartition(LongWritable key, LongWritable value,
				int numPartitions) {
			return (int) (((key.get() - BigDataApplication.MIN_ARTICLE_ID) / (BigDataApplication.MAX_ARTICLE_ID / numPartitions)) % numPartitions);
		}
	}

	/**
	 * Partitions article ids to the appropriate reducer
	 * 
	 * @author kurtp
	 *
	 */
	public static class Task3Partitioner extends
			Partitioner<LongWritable, RevisionTimeStampWritable> {

		@Override
		public int getPartition(LongWritable key,
				RevisionTimeStampWritable value, int numPartitions) {
			return (int) (((key.get() - BigDataApplication.MIN_ARTICLE_ID) / (BigDataApplication.MAX_ARTICLE_ID / numPartitions)) % numPartitions);
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
	 * Output Key Mapper object containing article Id and modifications
	 * 
	 * @author kurtp
	 *
	 */
	public static class ArticleIdModificationsWritable implements Writable,
			WritableComparable<ArticleIdModificationsWritable> {

		private Long articleId;
		private Integer modifications;

		public ArticleIdModificationsWritable() {
		}

		public ArticleIdModificationsWritable(long articleId, int modifications) {
			this.articleId = articleId;
			this.modifications = modifications;
		}

		@Override
		public String toString() {
			return (new StringBuilder().append(articleId)).append(" ")
					.append(modifications).toString();
		}

		public void readFields(DataInput dataInput) throws IOException {
			articleId = WritableUtils.readVLong(dataInput);
			modifications = WritableUtils.readVInt(dataInput);
		}

		public void write(DataOutput dataOutput) throws IOException {
			WritableUtils.writeVLong(dataOutput, articleId);
			WritableUtils.writeVInt(dataOutput, modifications);
		}

		public int compareTo(ArticleIdModificationsWritable objKeyPair) {
			int result = -modifications.compareTo(objKeyPair.modifications);
			if (result == 0) {
				result = articleId.compareTo(objKeyPair.articleId);
			}
			return result;
		}

		public Long getArticleId() {
			return articleId;
		}

		public void setArticleId(Long articleId) {
			this.articleId = articleId;
		}

		public Integer getModifications() {
			return modifications;
		}

		public void setModifications(Integer modifications) {
			this.modifications = modifications;
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
		String hdfs_home = "/user/2222148p/";
		conf.addResource(new Path("bd4-hadoop/conf/core-site.xml"));
		conf.set("mapred.jar", "/users/msc/2222148p/KurtJimmiBD.jar");
		// String inputLoc = "/user/bd4-ae1/enwiki-20080103-full.txt";
		String inputLoc = "/user/bd4-ae1/enwiki-20080103-sample.txt";
		String outputLoc = hdfs_home + "output";
		String tempLoc = hdfs_home + "temp";

		// localhost stuff
		/*
		 * conf.addResource(new Path("/etc/hadoop/conf.pseudo/core-site.xml"));
		 * String hdfs_home = "/user/hadoop/wiki/"; String inputLoc = hdfs_home
		 * + "wiki_1428.txt"; String outputLoc = hdfs_home + "output/"; String
		 * tempLoc = hdfs_home + "temp/";
		 */

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
			job.setPartitionerClass(Task1Partitioner.class);

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
			job.setOutputValueClass(IntWritable.class);
			break;
		case 1: // problem 3
			System.out.println("Task 3");

			conf.set(BigDataApplication.DATE_TO, args[0]);

			job = Job.getInstance(conf, "BigData3");

			job.setMapperClass(Task3Mapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(RevisionTimeStampWritable.class);
			job.setPartitionerClass(Task3Partitioner.class);

			job.setReducerClass(Task3Reducer.class);
			job.setOutputValueClass(Text.class);
			break;
		case 0:
			System.out.println("Get Max articleId");

			job = Job.getInstance(conf, "maxArticleId");

			job.setMapperClass(MaxArticleIdMapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(LongWritable.class);

			job.setReducerClass(MaxArticleIdReducer.class);
			job.setOutputValueClass(NullWritable.class);
			job.setNumReduceTasks(1);
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
				job2.setMapOutputKeyClass(ArticleIdModificationsWritable.class);
				job2.setMapOutputValueClass(NullWritable.class);
				job2.setOutputKeyClass(LongWritable.class);
				job2.setOutputValueClass(IntWritable.class);
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
