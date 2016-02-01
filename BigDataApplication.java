import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigDataApplication {

	public static final String DATE_FROM = "DATE_FROM";

	public static final String DATE_TO = "DATE_TO";

	public static final String K = "K";

	/**
	 * Gathers revision information from records
	 * 
	 * @author kurtp
	 */
	public static class BigDataAssignmentMapper
			extends
			Mapper<Object, Text, ArticleIdModificationsWritable, RevisionTimeStampWritable> {

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

			Revision r = new Revision();
			StringTokenizer words = new StringTokenizer(value.toString());

			// par each word in line
			while (words.hasMoreTokens()) {
				String word = words.nextToken();
				if ("REVISION".equals(word)) {

					// populate revision
					String tmpToken = "";
					while (words.hasMoreTokens()
							&& !"CATEGORY".equals(tmpToken = words.nextToken())) {
						r.setRevisionFields(tmpToken.trim());
					}
					mapRevision(r, dateTo, dateFrom, context);
				}
			}
		}

		/**
		 * Writes revision if it passes validation and time interval test
		 * 
		 * @author kurtp
		 */
		public void mapRevision(Revision r, Date dateTo, Date dateFrom,
				Context context) throws IOException, InterruptedException {

			// validate revision.. all fields exist
			if (r.isValid()) {
				// check if revision occurred in
				// time interval
				if (r.getTimeStamp().before(dateTo)
						&& (dateFrom == null || r.getTimeStamp()
								.after(dateFrom))) {
					context.write(
							new ArticleIdModificationsWritable(
									r.getArticleId(), 1),
							new RevisionTimeStampWritable(r.getRevisionId(), r
									.getTimeStampString()));
				}
			} else {
				System.out.println("Invalid Revision .. " + r.getRevisionId());
			}
		}

		/**
		 * Revision class
		 * 
		 * @author kurtp
		 */
		private class Revision {

			// article_id,revision_id,article title, timeStamp, ipUsername,
			// userId
			public String[] revisionField = new String[6];
			private int revCount = 0;

			public Revision() {

			}

			public String getTimeStampString() {
				return revisionField[3];
			}

			public void setRevisionFields(String value) {
				try {
					revisionField[revCount] = value;
					revCount++;
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out.println("Error adding: " + value + " to: "
							+ Arrays.toString(revisionField));
				}
			}

			public Long getArticleId() {
				return Long.parseLong(revisionField[0]);
			}

			public Long getRevisionId() {
				return Long.parseLong(revisionField[1]);
			}

			public Date getTimeStamp() {
				try {
					return Tools.getDateFromWikiString(revisionField[3]);
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

	/**
	 * Receives Articles with different revisions and timestamps. Sorts the
	 * revisions in this time interval
	 * 
	 * @author kurtp
	 *
	 */
	public static class Part1Reducer
			extends
			Reducer<ArticleIdModificationsWritable, RevisionTimeStampWritable, LongWritable, Text> {

		public void reduce(ArticleIdModificationsWritable key,
				Iterable<RevisionTimeStampWritable> values, Context context)
				throws IOException, InterruptedException {
			List<Long> revisions = new ArrayList<Long>();
			for (RevisionTimeStampWritable value : values) {
				revisions.add(value.getRevisionId());
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
			context.write(new LongWritable(key.getArticleId()), new Text(
					revisions.size() + " " + sb.toString()));
		}
	}

	/**
	 * Receives Articles with different revisions and timestamps. Adds them up
	 * together.
	 * 
	 * @author kurtp
	 *
	 */
	public static class Part2Combiner
			extends
			Reducer<ArticleIdModificationsWritable, RevisionTimeStampWritable, ArticleIdModificationsWritable, RevisionTimeStampWritable> {

		public void reduce(ArticleIdModificationsWritable key,
				Iterable<RevisionTimeStampWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			Long latestRevision = 0L;
			for (RevisionTimeStampWritable value : values) {
				sum++;
				latestRevision = value.getRevisionId();
			}

			context.write(new ArticleIdModificationsWritable(
					key.getArticleId(), sum), new RevisionTimeStampWritable(
					latestRevision.longValue(), ""));
		}
	}

	/**
	 * Receives Articles with different revisions and timestamps. Adds up the
	 * total number of modifications
	 * 
	 * @author kurtp
	 *
	 */
	public static class Part2Reducer
			extends
			Reducer<ArticleIdModificationsWritable, RevisionTimeStampWritable, LongWritable, LongWritable> {

		public void reduce(ArticleIdModificationsWritable key,
				Iterable<RevisionTimeStampWritable> values, Context context)
				throws IOException, InterruptedException {

			context.write(new LongWritable(key.getArticleId()),
					new LongWritable(key.getModifications()));
		}
	}

	public static class BigDataPart2SortMapper extends
			Mapper<Object, Text, ArticleIdModificationsWritable, NullWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String k = conf.get(BigDataApplication.K);

			StringTokenizer words = new StringTokenizer(value.toString());

			// par each word in line
			while (words.hasMoreTokens()) {
				String articleId = words.nextToken();
				String modifications = words.nextToken();
				context.write(
						new ArticleIdModificationsWritable(Long
								.parseLong(articleId), Long
								.parseLong(modifications)), NullWritable.get());
			}

		}
	}

	public static class SortPart2Reducer
			extends
			Reducer<ArticleIdModificationsWritable, NullWritable, LongWritable, LongWritable> {

		public void reduce(ArticleIdModificationsWritable key,
				Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(key.getArticleId()),
					new LongWritable(key.getModifications()));
		}
	}

	/**
	 * Receives Articles with different revisions and timestamps. Lists the
	 * revisions current at a point in time.
	 * 
	 * @author kurtp
	 *
	 */
	public static class Part3Reducer
			extends
			Reducer<ArticleIdModificationsWritable, RevisionTimeStampWritable, LongWritable, Text> {

		public void reduce(ArticleIdModificationsWritable key,
				Iterable<RevisionTimeStampWritable> values, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			Date dateTo = null;

			Date latestDate = null;
			Long revisionId = null;
			String timeStampResult = "";
			try {
				dateTo = Tools.getDateFromWikiString(conf
						.get(BigDataApplication.DATE_TO));

				for (RevisionTimeStampWritable value : values) {
					Date revTime = Tools.getDateFromWikiString(value
							.getTimeStamp());
					if (revTime.before(dateTo)) {
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
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
			context.write(new LongWritable(key.getArticleId()), new Text(
					revisionId + " " + timeStampResult));
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
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
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
		private Long modifications;

		public ArticleIdModificationsWritable() {
		}

		public ArticleIdModificationsWritable(long articleId, long modifications) {
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
			modifications = WritableUtils.readVLong(dataInput);
		}

		public void write(DataOutput dataOutput) throws IOException {
			WritableUtils.writeVLong(dataOutput, articleId);
			WritableUtils.writeVLong(dataOutput, modifications);
		}

		public int compareTo(ArticleIdModificationsWritable objKeyPair) {
			int result = modifications.compareTo(objKeyPair.modifications);
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

		public Long getModifications() {
			return modifications;
		}

		public void setModifications(Long modifications) {
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
		conf.addResource(new Path("bd4-hadoop/conf/core-site.xml"));
		conf.set("mapred.jar", "/users/msc/2222148p/KurtJimmiBD.jar");
		String inputLoc = "/user/bd4-ae1/";
		String inputFileName = "enwiki-20080103-full.txt";
		String outputLoc = "/user/2222148p/output";

		// localhost stuff
		// conf.addResource(new Path("/etc/hadoop/conf.pseudo/core-site.xml"));
		// String inputLoc = "/user/hadoop/wiki/";
		// String inputFileName = "wiki_1428.txt";
		// String outputLoc = "/user/hadoop/wiki/output/";

		String outputFileName = "part-r-00000";

		Job job = null;
		switch (args.length) {
		case 2: // problem 1
			System.out.println("Task 1");

			conf.set(BigDataApplication.DATE_FROM, args[0]);
			conf.set(BigDataApplication.DATE_TO, args[1]);

			job = Job.getInstance(conf, "BigData1");
			job.setReducerClass(Part1Reducer.class);
			job.setOutputValueClass(Text.class);
			break;
		case 3: // problem 2
			System.out.println("Task 2");

			conf.set(BigDataApplication.DATE_FROM, args[0]);
			conf.set(BigDataApplication.DATE_TO, args[1]);
			conf.set(BigDataApplication.K, args[2]);

			job = Job.getInstance(conf, "BigData2");
			job.setReducerClass(Part2Reducer.class);
			job.setCombinerClass(Part2Combiner.class);
			job.setOutputValueClass(LongWritable.class);
			break;
		case 1: // problem 3
			System.out.println("Task 3");

			conf.set(BigDataApplication.DATE_TO, args[0]);

			job = Job.getInstance(conf, "BigData3");
			job.setReducerClass(Part3Reducer.class);
			job.setOutputValueClass(Text.class);
			break;
		default:
			System.err.println("minimum 1 argument, maximum 3 arguments");
			System.exit(-1);
			break;
		}

		job.setJarByClass(BigDataApplication.class);
		job.setMapperClass(BigDataAssignmentMapper.class);
		job.setMapOutputKeyClass(ArticleIdModificationsWritable.class);
		job.setMapOutputValueClass(RevisionTimeStampWritable.class);
		job.setOutputKeyClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputLoc + inputFileName));
		FileOutputFormat.setOutputPath(job, new Path(outputLoc));

		// waiting for job to finish
		boolean job1FinishedCorrectly = job.waitForCompletion(true);

		if (job1FinishedCorrectly) {
			// check if it is problem 2
			if (args.length == 3) {
				// start job that does the sorting
				Job job2 = Job.getInstance(conf, "BigData2Sorting");
				job2.setReducerClass(SortPart2Reducer.class);
				job2.setJarByClass(BigDataApplication.class);
				job2.setMapperClass(BigDataPart2SortMapper.class);
				job2.setMapOutputKeyClass(ArticleIdModificationsWritable.class);
				job2.setMapOutputValueClass(NullWritable.class);
				job2.setOutputKeyClass(LongWritable.class);
				job2.setOutputValueClass(LongWritable.class);

				FileInputFormat.addInputPath(job2, new Path(outputLoc
						+ outputFileName));
				FileOutputFormat.setOutputPath(job2, new Path(inputLoc
						+ "sortedOutput/"));
				// wait for sorting to finish
				System.exit(job2.waitForCompletion(true) ? 0 : 1);

			}
			System.exit(0);
		}
		System.exit(1);
	}
}
