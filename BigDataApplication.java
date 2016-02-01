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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigDataApplication {

	public static final String DATE_FROM = "DATE_FROM";

	public static final String DATE_TO = "DATE_TO";

	public static final String K = "K";

	public static final int RECORD_LENGTH = 14;

	public static final int RECORDS_PER_MAPPER = 3000;

	public static class BigDataAssignmentMapper
			extends
			Mapper<Object, Text, CompositeKeyWritable, RevisionTimeStampWritable> {

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
							new CompositeKeyWritable(r.getArticleId()),
							new RevisionTimeStampWritable(r.getRevisionId(), r
									.getTimeStampString()));
				}
			} else {
				System.out.println("Invalid Revision .. " + r.getRevisionId());
			}
		}

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
				revisionField[revCount] = value;
				revCount++;
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

	public static class Part1Reducer
			extends
			Reducer<CompositeKeyWritable, RevisionTimeStampWritable, LongWritable, Text> {

		public void reduce(CompositeKeyWritable key,
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

	public static class Part2Reducer
			extends
			Reducer<CompositeKeyWritable, RevisionTimeStampWritable, LongWritable, IntWritable> {

		public void reduce(CompositeKeyWritable key,
				Iterable<RevisionTimeStampWritable> values, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String k = conf.get(BigDataApplication.K);

			int sum = 0;
			for (RevisionTimeStampWritable value : values) {
				sum++;
			}
			context.write(new LongWritable(key.getArticleId()),
					new IntWritable(sum));
		}
	}

	public static class Part3Reducer
			extends
			Reducer<CompositeKeyWritable, RevisionTimeStampWritable, LongWritable, Text> {

		public void reduce(CompositeKeyWritable key,
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

	public static class Tools {

		private Tools() {

		}

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

	public static class RevisionTimeStampWritable implements Writable,
			WritableComparable<CompositeKeyWritable> {

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

		public int compareTo(CompositeKeyWritable objKeyPair) {
			int result = revisionId.compareTo(objKeyPair.articleId);
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

	public static class CompositeKeyWritable implements Writable,
			WritableComparable<CompositeKeyWritable> {

		private Long articleId;

		public CompositeKeyWritable() {
		}

		public CompositeKeyWritable(long articleId) {
			this.articleId = articleId;
		}

		@Override
		public String toString() {
			return (new StringBuilder().append(articleId)).toString();
		}

		public void readFields(DataInput dataInput) throws IOException {
			articleId = WritableUtils.readVLong(dataInput);
		}

		public void write(DataOutput dataOutput) throws IOException {
			WritableUtils.writeVLong(dataOutput, articleId);
		}

		public int compareTo(CompositeKeyWritable objKeyPair) {
			int result = articleId.compareTo(objKeyPair.articleId);
			return result;
		}

		public Long getArticleId() {
			return articleId;
		}

		public void setArticleId(Long articleId) {
			this.articleId = articleId;
		}
	}

	public static class SecondarySortBasicPartitioner extends
			Partitioner<CompositeKeyWritable, Text> {

		@Override
		public int getPartition(CompositeKeyWritable key, Text value,
				int numReduceTasks) {

			return (key.getArticleId().hashCode() % numReduceTasks);
		}
	}

	public static class SecondarySortBasicCompKeySortComparator extends
			WritableComparator {

		protected SecondarySortBasicCompKeySortComparator() {
			super(CompositeKeyWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
			CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

			int cmpResult = key1.getArticleId().compareTo(key2.getArticleId());
			return cmpResult;
		}
	}

	public static class SecondarySortBasicGroupingComparator extends
			WritableComparator {
		protected SecondarySortBasicGroupingComparator() {
			super(CompositeKeyWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
			CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
			return key1.getArticleId().compareTo(key2.getArticleId());
		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println("BigDataApplication Started");
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf.pseudo/core-site.xml"));

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

			job.setMapperClass(BigDataAssignmentMapper.class);
			job.setMapOutputKeyClass(CompositeKeyWritable.class);
			job.setMapOutputValueClass(RevisionTimeStampWritable.class);
			job.setPartitionerClass(SecondarySortBasicPartitioner.class);
			job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
			job.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
			job.setReducerClass(Part1Reducer.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			break;
		case 3: // problem 2
			System.out.println("Prob 2");

			conf.set(BigDataApplication.DATE_FROM, args[0]);
			conf.set(BigDataApplication.DATE_TO, args[1]);
			conf.set(BigDataApplication.K, args[2]);

			job = Job.getInstance(conf, "BigData2");
			job.setJarByClass(BigDataApplication.class);

			job.setMapperClass(BigDataAssignmentMapper.class);
			job.setMapOutputKeyClass(CompositeKeyWritable.class);
			job.setMapOutputValueClass(RevisionTimeStampWritable.class);
			job.setPartitionerClass(SecondarySortBasicPartitioner.class);
			job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
			job.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
			job.setReducerClass(Part2Reducer.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(IntWritable.class);
			break;
		case 1: // problem 3
			System.out.println("Prob 3");

			conf.set(BigDataApplication.DATE_TO, args[0]);

			job = Job.getInstance(conf, "BigData3");
			job.setJarByClass(BigDataApplication.class);

			job.setMapperClass(BigDataAssignmentMapper.class);
			job.setMapOutputKeyClass(CompositeKeyWritable.class);
			job.setMapOutputValueClass(RevisionTimeStampWritable.class);
			job.setPartitionerClass(SecondarySortBasicPartitioner.class);
			job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
			job.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
			job.setReducerClass(Part3Reducer.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			break;
		default:
			System.err.println("minimum 1 argument, maximum 3 arguments");
			System.exit(-1);
			break;
		}

		FileInputFormat.addInputPath(job, new Path(inputLoc));
		FileOutputFormat.setOutputPath(job, new Path(outputLoc));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
