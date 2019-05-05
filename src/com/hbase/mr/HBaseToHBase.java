package com.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

public class HBaseToHBase {

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
		conf = HBaseConfiguration.create(conf);

		Job job = Job.getInstance(conf);
		job.setJarByClass(HBaseToHBase.class);
		job.setJobName("HBaseToHBase");

		Scan scan = new Scan();
		//scan.setStartRow("1001".getBytes());
		//scan.setStopRow("1003".getBytes());
		// 还可以设置一些过滤的东西：
		// scan.setFilter(filter)
		scan.setCacheBlocks(false);
		scan.setCaching(500);

		TableMapReduceUtil.initTableMapperJob("hdfs_to_hbase_no_reducer", scan, HBaseToHBaseMapper.class,
				ImmutableBytesWritable.class, Put.class, job, false);

		// 指定reducer类，和hbase输出table, 依赖jar设置为false
		TableMapReduceUtil.initTableReducerJob("hbase_to_hbase", HBaseToHBaseReducer.class, job, null, null, null, null, false);

		if (job.waitForCompletion(true)) {
			System.err.println("ok");
		}
	}

	static class HBaseToHBaseMapper extends TableMapper<ImmutableBytesWritable, Put> {
		byte[] family = Bytes.toBytes("cf1");
		byte[] nameCol = Bytes.toBytes("name");
		byte[] ageCol = Bytes.toBytes("age");
		byte[] heightCol = Bytes.toBytes("height");

		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
			byte[] name = result.getValue(family, nameCol);
			byte[] age = result.getValue(family, ageCol);
			byte[] height = result.getValue(family, heightCol);
			
			System.err.println(Bytes.toString(key.get()));
			
			Put put = new Put(key.get());
			put.add(family, nameCol, name);
			put.add(family, ageCol, age);
			put.add(family, heightCol, height);
			
			//从Hbase读取的数据写入到Hbase,没有reducer的不能用NullWritable类型，否则会覆盖，不知道为什么
			context.write(key, put);
			System.err.println("output" + Bytes.toString(key.get()));
			
		}
	}
	
	static class HBaseToHBaseReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
		NullWritable rkey = NullWritable.get();
		
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<Put> puts, Context context) throws IOException, InterruptedException {
			for (Put put : puts) {
				context.write(rkey, put);
			}
		}
	}
}

