import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Assignment2 extends Configured implements Tool {

	public static final String DATE_FROM = "DATE_FROM";

	public static final String DATE_TO = "DATE_TO";

	public static final String K = "K";

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Assignment2(), args));
	}

	public int run(String[] args) throws Exception {
		// String tableName = "BD4Project2";
		String tableName = "BD4Project2Sample";
		String outputTable = "2222148p";
		String columnFamily = "WD";

		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("bd4-hadoop/conf/core-site.xml"));
		// conf.set("mapred.jar", "file:///path/to/my.jar");

		// create filters.. we only need key
		FilterList allFilters = new FilterList(Operator.MUST_PASS_ALL);
		allFilters.addFilter(new KeyOnlyFilter());

		Scan scan = new Scan();
		scan.setCaching(100);
		scan.setCacheBlocks(false);
		scan.setFilter(allFilters);
		scan.setMaxVersions(1);

		Job job = null;
		switch (args.length) {
		case 2: // problem 1
			System.out.println("Task 1");
			scan.setTimeRange(Tools.getDateFromWikiString(args[0]),
					Tools.getDateFromWikiString(args[1]));

			job = Job.getInstance(conf, "BigData1");

			TableMapReduceUtil.initTableMapperJob(tableName, scan,
					Task1Mapper.class, ImmutableBytesWritable.class,
					IntWritable.class, job);

			TableMapReduceUtil.initTableReducerJob(outputTable,
					Task1Reducer.class, job);

			break;
		case 3: // problem 2
			System.out.println("Task 2");
			scan.setTimeRange(Tools.getDateFromWikiString(args[0]),
					Tools.getDateFromWikiString(args[1]));

			conf.set(Assignment2.K, args[2]);
			job = Job.getInstance(conf, "BigData2");

			break;
		case 1: // problem 3
			System.out.println("Task 3");
			scan.setTimeRange(0L, Tools.getDateFromWikiString(args[0]));

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
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public class Task1Mapper extends
			TableMapper<ImmutableBytesWritable, IntWritable> {

		public void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {

			// extract info
			long articleId = Bytes.readVLong(key.get(), 0);
			long revisionId = Bytes.readVLong(key.get(), 8);
			// write info
			context.write(new ImmutableBytesWritable(Bytes.toBytes(articleId)),
					new IntWritable((int) revisionId));
		}
	}

	public class Task1Reducer
			extends
			TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key,
				Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// sort revisions of article id
			List<Integer> revisions = new ArrayList<Integer>();
			for (IntWritable value : values) {
				revisions.add(value.get());
			}
			Collections.sort(revisions);

			// populate output row
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("Task1"), Bytes.toBytes("total"),
					Bytes.toBytes(revisions.size()));
			for (int i = 0; i < revisions.size(); i++) {
				// column family, column name, data
				put.add(Bytes.toBytes("Task1"), Bytes.toBytes("rev_" + i),
						Bytes.toBytes(revisions.get(i)));
			}
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
		public static long getDateFromWikiString(String wikiText)
				throws ParseException {
			if (wikiText == null) {
				System.err.println("ERROR: Trying to parse NULL Date");
				return 0;
			}
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			df.setTimeZone(TimeZone.getTimeZone("UTC"));
			return df.parse(wikiText.replace("T", " ").replace("Z", ""))
					.getTime();
		}
	}

}
