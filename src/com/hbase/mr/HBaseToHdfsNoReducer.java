package com.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HBaseToHdfsNoReducer {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node001:8020");
		// conf.set("yarn.resourcemanager.hostname", "node003");
		conf.set("hbase.zookeeper.quorum", "node004");
		// linux平台外跑的时候设置为true, 否则可能报错
		conf.set("mapreduce.app-submission.corss-paltform", "true");
		// 设置为本地模式
		conf.set("mapreduce.framework.name", "local");

		// conf重新赋值
		//conf = HBaseConfiguration.create(conf);

		Job job = Job.getInstance(conf);
		job.setJarByClass(HBaseToHdfsNoReducer.class);
		job.setJobName("HBaseToHdfsNoReducer");

		Scan scan = new Scan();
		//scan.setStartRow("1001".getBytes());
		//scan.setStopRow("1003".getBytes());
		// 还可以设置一些过滤的东西：
		// scan.setFilter(filter)
		scan.setCacheBlocks(false);
		scan.setCaching(500);

		TableMapReduceUtil.initTableMapperJob("hdfs_to_hbase_no_reducer", scan, HBaseToHdfsNoReducerMapper.class,
				NullWritable.class, Text.class, job, false);
		
		//没有reduce最简单的办法是设置为0
		job.setNumReduceTasks(0);
		
		FileOutputFormat.setOutputPath(job, new Path("hdfs://node001:8020/user/root/hbase/hbase_to_hbase.csv"));
		
		
		
		if (job.waitForCompletion(true)) {
			System.err.println("ok");
		}
	}

	static class HBaseToHdfsNoReducerMapper extends TableMapper<NullWritable, Text> {
		byte[] family = Bytes.toBytes("cf1");
		byte[] nameCol = Bytes.toBytes("name");
		byte[] ageCol = Bytes.toBytes("age");
		byte[] heightCol = Bytes.toBytes("height");
		
		NullWritable mkey = NullWritable.get();
		Text mval = new Text();
		
		
		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
			byte[] name = result.getValue(family, nameCol);
			byte[] age = result.getValue(family, ageCol);
			byte[] height = result.getValue(family, heightCol);
			
			mval.set(Bytes.toString(name) + "," + Bytes.toString(age) + "," + Bytes.toString(height));
			context.write(mkey, mval);
			
		}
	}
}

