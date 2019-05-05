package com.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class HdfsToHBase {
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
		job.setJarByClass(HdfsToHBase.class);
		job.setJobName("HdfsToHBase");
		
		//从hdfs得到数据
		Path inputPath = new Path("hdfs://node001:8020/user/root/HBase/HBase.txt");
		FileInputFormat.addInputPath(job, inputPath);

		job.setMapperClass(HdfsToHBaseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//指定reducer类，和hbase输出table, 依赖jar设置为false
		TableMapReduceUtil.initTableReducerJob("hdfs_to_hbase", HdfsToHBaseReducer.class, job, null, null, null, null, false);
		
		if(job.waitForCompletion(true)) {
			System.err.println("ok");
		}
	}
	
	/**
	 * 直接输出什么都没有干
	 * @author ZORO
	 *
	 */
	static class HdfsToHBaseMapper extends Mapper<Object, Text, Text, NullWritable>{
		NullWritable mval = NullWritable.get();
		@Override
		protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
			context.write(value, mval);
		}
	}
	
	
	static class HdfsToHBaseReducer extends TableReducer<Text, NullWritable, NullWritable>{
		NullWritable rkey = NullWritable.get();
		Put put;
		byte[] family = Bytes.toBytes("cf1");
		byte[] nameCol = Bytes.toBytes("name");
		byte[] ageCol = Bytes.toBytes("age");
		byte[] heightCol = Bytes.toBytes("height");
		@Override
		protected void reduce(Text text, Iterable<NullWritable> val, Context context) throws IOException, InterruptedException {
			
			for (NullWritable nullWritable : val) {
				String[] split = text.toString().split(",");
				if ( split.length == 4) {
					//封装到put中，rowkey,列族：列，val
					//写到哪张表中，运行类里面已经有TableMapReduceUtil.initTableReducerJob
					byte[] rowkey = split[0].getBytes();
					put = new Put(rowkey);
					put.add(family, nameCol, Bytes.toBytes(split[1]));
					put.add(family, ageCol, Bytes.toBytes(split[1]));
					put.add(family, heightCol, Bytes.toBytes(split[2]));
					
					context.write(rkey, put);
				}
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
}
