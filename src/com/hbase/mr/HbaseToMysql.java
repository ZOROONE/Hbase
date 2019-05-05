package com.hbase.mr;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class HbaseToMysql {

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
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.0.11:3306/test", "root", "123");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(HbaseToMysql.class);
		job.setJobName("HbaseToMysql");
		
		
		
		Scan scan = new Scan();
		//scan.setStartRow("1001".getBytes());
		//scan.setStopRow("1003".getBytes());
		// 还可以设置一些过滤的东西：
		// scan.setFilter(filter)
		scan.setCacheBlocks(false);
		scan.setCaching(500);

		TableMapReduceUtil.initTableMapperJob("hdfs_to_hbase_no_reducer", scan, HbaseToMysqlMapper.class,
				UserInfo.class, NullWritable.class, job, false);
		
		//没有reduce最简单的办法是设置为0
		job.setNumReduceTasks(0);
		
		DBOutputFormat.setOutput(job, "hbase_to_mysql", "id", "name", "age");
		
		
		if (job.waitForCompletion(true)) {
			System.err.println("ok");
		}
	}

	static class HbaseToMysqlMapper extends TableMapper<UserInfo, NullWritable> {
		byte[] family = Bytes.toBytes("cf1");
		byte[] nameCol = Bytes.toBytes("name");
		byte[] ageCol = Bytes.toBytes("age");
		byte[] heightCol = Bytes.toBytes("height");
		
		UserInfo mkey = new UserInfo();
		NullWritable mval = NullWritable.get();
		
		
		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
			byte[] name = result.getValue(family, nameCol);
			byte[] age = result.getValue(family, ageCol);
			
			mkey.setAge(Bytes.toString(age));
			mkey.setName(Bytes.toString(name));
			mkey.setId(Bytes.toString(key.get()));
			context.write(mkey, mval);
			
		}
	}
	
	static class UserInfo implements DBWritable{
		
		String id;
		String name;
		String age;
		
		
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getAge() {
			return age;
		}

		public void setAge(String age) {
			this.age = age;
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1, this.id);
			statement.setString(2, this.name);
			statement.setString(3, this.age);
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			resultSet.getString(1);
			resultSet.getString(2);
			resultSet.getString(3);
			
		}
	}
}

