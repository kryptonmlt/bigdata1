import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Assignment2(), args));
	}
	
	public int run(String[] arg0) throws Exception {
		// String tableName = "BD4Project2";
		String tableName = "BD4Project2Sample";
		String columnFamily = "WD";

		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("bd4-hadoop/conf/core-site.xml"));
		// conf.set("mapred.jar", "file:///path/to/my.jar");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(Assignment2.class);
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("A"), Bytes.toBytes("name"));
		scan.setCaching(100);
		scan.setCacheBlocks(false);
		// Always set this to false for MR jobs!
		TableMapReduceUtil.initTableMapperJob(tableName, scan, MyNameCounterMapper.class,
				ImmutableBytesWritable.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName, MyNameCounterReducer.class, job);
		job.setNumReduceTasks(30);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public class MyNameCounterMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
		private final IntWritable one = new IntWritable(1);

		public void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			byte[] name = value.getValue(Bytes.toBytes("A"), Bytes.toBytes("name"));
			if (name != null) {
				context.write(new ImmutableBytesWritable(name), one);
			}
		}
	}

	public class MyNameCounterReducer
			extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("A"), key.get(), Bytes.toBytes(sum));
			context.write(new ImmutableBytesWritable(put.getRow()), put);
		}
	}

}
