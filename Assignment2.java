package assignment2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class Assignment2 extends Configured {
	
	public static void main(String[] args) throws IOException {
		//String tableName = "BD4Project2";
		String tableName = "BD4Project2Sample";
		String columnFamily = "WD";
		
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("bd4-hadoop/conf/client-conf.xml"));
		// conf.set("mapred.jar", "file:///path/to/my.jar");
		Job job = Job.getInstance(conf);

		HTable table = new HTable(conf, tableName);
		
		//count example below
		long count = 0;
		try {
			ResultScanner scanner = table.getScanner(Bytes
					.toBytes(columnFamily));
			for (Iterator<Result> iterator = scanner.iterator(); iterator
					.hasNext(); iterator.next())
				count++;
			scanner.close();
		} finally {
			table.close();
		}
		System.out.println(count);
	}
}
