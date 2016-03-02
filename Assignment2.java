import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.apache.directory.api.util.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Assignment2 extends Configured implements Tool {

	public static final String K = "K";
	public static final String Task1_CF = "Task1";
	public static final String Task2_CF = "Task2";
	public static final String Task3_CF = "Task3";

	public static final String INPUT_TABLE = "BD4Project2Sample";
	public static final String OUTPUT_TABLE = "2222148p";
	public static final String INPUT_CF = "WD";

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Assignment2(), args));
	}

	public int run(String[] args) throws Exception {
		// String tableName = "BD4Project2";

		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("bd4-hadoop/conf/core-site.xml"));
		conf.set("mapred.jar", "file:/users/msc/2222148p/KurtJimmi2.jar");

		// create filters.. we only need key
		FilterList allFilters = new FilterList(Operator.MUST_PASS_ALL);
		allFilters.addFilter(new KeyOnlyFilter());

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(Assignment2.INPUT_CF));
		scan.setCaching(100);
		scan.setCacheBlocks(false);
		scan.setFilter(allFilters);
		scan.setMaxVersions(1);

		HBaseAdmin admin = new HBaseAdmin(conf);

		Job job = null;
		switch (args.length) {
		case 2: // problem 1

			System.out.println("Task 1");
			scan.setTimeRange(Tools.getDateFromWikiString(args[0]), Tools.getDateFromWikiString(args[1]));

			job = Job.getInstance(conf, "BigData1");
			prepareOutputTable(admin, Assignment2.Task1_CF);

			TableMapReduceUtil.initTableMapperJob(Assignment2.INPUT_TABLE, scan, Task1Mapper.class,
					ImmutableBytesWritable.class, LongWritable.class, job);
			TableMapReduceUtil.initTableReducerJob(Assignment2.OUTPUT_TABLE, Task1Reducer.class, job);
			System.out.println("Task 1 config set");

			break;
		case 3: // problem 2
			System.out.println("Task 2");
			scan.setTimeRange(Tools.getDateFromWikiString(args[0]), Tools.getDateFromWikiString(args[1]));
			prepareOutputTable(admin, Assignment2.Task2_CF);
			job = Job.getInstance(conf, "BigData2");

			TableMapReduceUtil.initTableMapperJob(Assignment2.INPUT_TABLE, scan, Task2Mapper.class,
					ImmutableBytesWritable.class, IntWritable.class, job);
			TableMapReduceUtil.initTableReducerJob(Assignment2.OUTPUT_TABLE, Task2Reducer.class, job);
			System.out.println("Task 2 config set");

			break;
		case 1: // problem 3
			System.out.println("Task 3");
			scan.setTimeRange(0L, Tools.getDateFromWikiString(args[0]));
			prepareOutputTable(admin, Assignment2.Task3_CF);

			job = Job.getInstance(conf, "BigData3");

			break;
		case 0:// job to get maximum and minimum id
			System.out.println("Get Max articleId");

			job = Job.getInstance(conf, "maxArticleId");
			break;
		default:
			System.err.println("minimum 1 argument, maximum 3 arguments");
			System.exit(-1);
			break;
		}

		job.setJarByClass(Assignment2.class);
		int result = job.waitForCompletion(true) ? 0 : 1;
		switch (args.length) {
		case 2: // problem 1
			viewData(conf, Assignment2.Task1_CF, 0);
			break;
		case 3:// problem 2
			viewData(conf, Assignment2.Task2_CF, Integer.parseInt(args[2]));
			break;
		case 1:// problem 3
			viewData(conf, Assignment2.Task3_CF, 0);
			break;
		default:
			System.out.println("Unkown Task..");
			break;
		}
		return result;
	}

	public static void viewData(Configuration conf, String columnFamily, int k) throws IOException {

		HTable table = new HTable(conf, Assignment2.OUTPUT_TABLE);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(columnFamily));
		ResultScanner ss = table.getScanner(scan);
		switch (columnFamily) {
		case Assignment2.Task1_CF:
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					long key = Bytes.toLong(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
					byte[] data = Bytes.copy(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
					int revisionSize = kv.getValueLength() / 8;

					StringBuffer sb = new StringBuffer();
					for (int i = 0; i < revisionSize; i++) {
						sb.append(" " + Bytes.toLong(data, 8 * i, 8));
					}
					System.out.println(key + " " + revisionSize + " " + sb.toString());
				}

			}
			break;
		case Assignment2.Task2_CF:
			int i = 0;
			for (Result r : ss) {
				if (i > k) {
					break;
				}
				for (KeyValue kv : r.raw()) {
					int inverseAmount = Bytes.toInt(kv.getBuffer(), kv.getRowOffset(), 4);
					int amount = Integer.MAX_VALUE - inverseAmount;
					long key = Bytes.toLong(kv.getBuffer(), (kv.getRowOffset() + 4), 8);
					System.out.println(key + " " + amount);
				}
				i++;
			}
			break;
		case Assignment2.Task3_CF:
			break;
		default:
			break;
		}
		table.close();
	}

	public static void prepareOutputTable(HBaseAdmin admin, String columnFamily) throws IOException {
		// Deleting a column family
		try {
			admin.deleteColumn(Assignment2.OUTPUT_TABLE, columnFamily);
			System.out.println("Column Family " + columnFamily + " deleted ..");
		} catch (InvalidFamilyOperationException e) {
			System.out.println("Column Family " + columnFamily + " already deleted ..");
		}
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);

		// Adding column family
		admin.addColumn(Assignment2.OUTPUT_TABLE, columnDescriptor);
		System.out.println("Column Family " + columnFamily + " added");
	}

	public static class Task1Mapper extends TableMapper<ImmutableBytesWritable, LongWritable> {

		public void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {

			// extract info
			long articleId = Bytes.toLong(key.get(), 0, 8);
			long revisionId = Bytes.toLong(key.get(), 8, 8);
			// write info
			context.write(new ImmutableBytesWritable(Bytes.toBytes(articleId)), new LongWritable(revisionId));
		}
	}

	public static class Task1Reducer
			extends TableReducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			// sort revisions of article id
			List<Long> revisions = new ArrayList<Long>();
			for (LongWritable value : values) {
				revisions.add(value.get());
			}
			Collections.sort(revisions);

			// populate buffer
			ByteBuffer result = new ByteBuffer(5);
			// result.append(Bytes.toBytes(revisions.size()));
			for (int i = 0; i < revisions.size(); i++) {
				result.append(Bytes.toBytes(revisions.get(i)));
			}

			Put put = new Put(key.get());
			// column family, column name, data
			put.add(Bytes.toBytes(Assignment2.Task1_CF), Bytes.toBytes("result"), result.buffer());
			// write row
			context.write(new ImmutableBytesWritable(put.getRow()), put);
		}
	}

	public static class Task2Mapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
		private static IntWritable one = new IntWritable(1);

		public void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {

			// extract info
			long articleId = Bytes.toLong(key.get(), 0, 8);
			// write info
			context.write(new ImmutableBytesWritable(Bytes.toBytes(articleId)), one);
		}
	}

	public static class Task2Reducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// sort revisions of article id
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			int inverseSum = Integer.MAX_VALUE - sum;
			Put put = new Put(Bytes.add(Bytes.toBytes(inverseSum), key.get()));
			// column family, column name, data
			put.add(Bytes.toBytes(Assignment2.Task2_CF), Bytes.toBytes("result"), Bytes.toBytes(true));
			// write row
			context.write(new ImmutableBytesWritable(put.getRow()), put);
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
		public static long getDateFromWikiString(String wikiText) throws ParseException {
			if (wikiText == null) {
				System.err.println("ERROR: Trying to parse NULL Date");
				return 0;
			}
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			df.setTimeZone(TimeZone.getTimeZone("UTC"));
			return df.parse(wikiText.replace("T", " ").replace("Z", "")).getTime();
		}
	}

}
