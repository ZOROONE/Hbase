package com.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * this is study for hbase mr
 * 
 * @author ZORO
 *
 */
public class HdfsToHbaseNoReducer {

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
		job.setJarByClass(HdfsToHbaseNoReducer.class);
		job.setJobName("HdfsToHbaseNoReducer");

		Path inputPath = new Path("hdfs://node001:8020/user/root/HBase/HBase.txt");
		FileInputFormat.addInputPath(job, inputPath);

		job.setMapperClass(HdfsToHbaseNoReduceMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);

		// TableMapReduceUtil.initTableReducerJob(table, reducer, job,
		// partitioner, quorumAddress, serverClass, serverImpl,
		// addDependencyJars);
		// 没有reduce,所以设置为null, 最主要的是addDependencyJars， 如果本地模式，设置为false
		TableMapReduceUtil.initTableReducerJob("hdfs_to_hbase_no_reducer", null, job, null, null, null, null, false);

		boolean flag = job.waitForCompletion(true);
		if (flag) {
			System.err.println("mr 结束");
		}
		
	}
	
	static class HdfsToHbaseNoReduceMapper extends Mapper<Object, Text, NullWritable, Put> {
		
		NullWritable mkey = NullWritable.get();
		Put put;
		byte[] family = Bytes.toBytes("cf1");
		byte[] nameCol = Bytes.toBytes("name");
		byte[] ageCol = Bytes.toBytes("age");
		byte[] heightCol = Bytes.toBytes("height");
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			if ( split.length == 4) {
				//封装到put中，rowkey,列族：列，val
				//写到哪张表中，运行类里面已经有TableMapReduceUtil.initTableReducerJob
				byte[] rowkey = split[0].getBytes();
				put = new Put(rowkey);
				put.add(family, nameCol, Bytes.toBytes(split[1]));
				put.add(family, ageCol, Bytes.toBytes(split[2]));
				put.add(family, heightCol, Bytes.toBytes(split[3]));
				
				context.write(mkey, put);
			}
		}
		
	}
}
