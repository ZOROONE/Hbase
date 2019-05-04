package com.hbase.test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseDemo {

	HBaseAdmin hadmin;
	HTable htable;
	String tn = "phone";
	byte[] family = "cf".getBytes();

	@Before
	public void begin() throws Exception {
		Configuration conf = new Configuration();
		// 伪分布式，
		conf.set("hbase.zookeeper.quorum", "node004");
		// 分布式需要指定zookeeper集群
		// conf.set("hbase.zookeeper.quorum", "node1,node2,node3");

		hadmin = new HBaseAdmin(conf);
		// 用htable对表进行操作
		htable = new HTable(conf, tn);
	}

	@After
	public void after() throws IOException {
		if (hadmin != null) {
			hadmin.close();
		}

		if (htable != null) {
			htable.close();
		}
	}

	@Test
	public void createTable() throws Exception {
		// 如果表已经存在，先删除
		if (hadmin.tableExists(tn)) {
			// 先disable表
			hadmin.disableTable(tn);
			// 在删除表
			hadmin.deleteTable(tn);
		}

		// 创建表
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tn));

		// 创建列族 cf
		HColumnDescriptor cf = new HColumnDescriptor(family);
		// 读缓存默认不作用于内存
		cf.setInMemory(true);
		// 往表中添加列族
		desc.addFamily(cf);
		// 创建表
		hadmin.createTable(desc);
	}

	@Test
	public void innerdb() throws Exception {

		String rowkey = "123123";
		// 创建一个指定往哪个rowkey中插入数据
		Put put = new Put(rowkey.getBytes());

		// add(byte [] family, byte [] qualifier, byte [] value)
		// 列族， 列名， 值
		put.add(family, "name".getBytes(), "zhangsan".getBytes());
		put.add(family, "age".getBytes(), "18".getBytes());

		// 插入数据到哪个表， 如果插入许多数据呢List<Put> puts;
		htable.put(put);
	}

	@Test
	public void getdb() throws IOException {

		// 指定rowkey
		String rowkey = "123123";

		Get get = new Get(rowkey.getBytes());
		// 添加get条件，指定到列名，则取到的只有该列数据，
		// 下面如果想要输出age列数据就会报错，可以不添加，添加提高查询效率
		get.addColumn(family, "name".getBytes());
		get.addColumn(family, "age".getBytes());
		// 指定表进行操作，查到数据， 如果多个get, 则List<Get> gets;
		Result result = htable.get(get);

		// 取到具体数据
		Cell nameCell = result.getColumnLatestCell(family, "name".getBytes());
		Cell ageCell = result.getColumnLatestCell(family, "age".getBytes());

		// 打印值
		System.out.println(new String(CellUtil.cloneValue(nameCell)));
		System.out.println(new String(CellUtil.cloneValue(ageCell)));
	}

	/**
	 * 通话详单： 手机号 对方手机号 通话时间 通话时长 主叫被叫 查询通话详：查询某一个月 某个时间段 所有的通话记录（时间降序）
	 * Rowkey：手机号_（Long.max-通话时间)
	 */

	HTools htools = new HTools();
	Random r = new Random();

	/**
	 * 十个用户 每个用户产生一百条通话记录
	 * 
	 * @throws Exception
	 */
	@Test
	public void insertDB2() throws Exception {
		List<Put> puts = new ArrayList<Put>();

		for (int i = 0; i < 10; i++) {
			// 本机手机号
			String pnum = htools.getPhoneNum("186");

			for (int j = 0; j < 100; j++) {
				// 对方手机号前缀
				String dnum = htools.getPhoneNum("177");
				// 指定年份，生成时间字符串yyyyMMddHHmmss
				String datestr = htools.getDate("2017");
				// 指定通话时长，随机数
				String length = r.nextInt(99) + "";
				// 指定通话类型
				String type = r.nextInt(2) + "";

				// 时间format
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				// Rowkey：手机号_（Long.max-通话时间)，为了让最新的通话数据再最上面，按时间倒序
				String rowkey = pnum + "_" + (Long.MAX_VALUE - sdf.parse(datestr).getTime());
				Put put = new Put(rowkey.getBytes());

				// 列 对方号码 family:dnum
				put.add(family, "dnum".getBytes(), dnum.getBytes());
				// 通话日期 yyyyMMddHHmmss
				put.add(family, "date".getBytes(), datestr.getBytes());
				// 指定通话时长
				put.add(family, "length".getBytes(), length.getBytes());
				// 通话类型，主叫，被叫
				put.add(family, "type".getBytes(), type.getBytes());
				// 将操作添加到集合中
				puts.add(put);
			}
		}

		// 对哪个表进行操作
		htable.put(puts);
	}

	/**
	 * 查询某个手机号 某月产生的所有的通话记录 18692952699 5月份 5.1-6.1
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDB() throws Exception {
		// 创建一个扫描
		Scan scan = new Scan();

		// 范围查找, 指定开始和结束rowkey
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String startRow = "18692952699_" + (Long.MAX_VALUE - sdf.parse("20170601000000").getTime());
		String stopRow = "18692952699_" + (Long.MAX_VALUE - sdf.parse("20170501000000").getTime());

		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());

		// 指定要扫描的哪些列，这样可以加快查找速度，
		// 也可以只指定查找的哪些列族，
		// 也可以什么都不指定，这样查找每个rowkey下的所有列族下的所有数据
		scan.addColumn(family, "dnum".getBytes());
		scan.addColumn(family, "date".getBytes());
		scan.addColumn(family, "length".getBytes());
		scan.addColumn(family, "type".getBytes());

		// 对哪个表进行扫描查找
		ResultScanner scanner = htable.getScanner(scan);

		for (Result result : scanner) {
			String dnumValue = new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "dnum".getBytes())));
			String dateValue = new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "date".getBytes())));
			String lengthValue = new String(
					CellUtil.cloneValue(result.getColumnLatestCell(family, "length".getBytes())));
			String typeValue = new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "type".getBytes())));

			System.out.println("dnum :" + dnumValue + "\tdate :" + dateValue + "\tlength :" + lengthValue + "\ttype :"
					+ typeValue);
		}
	}

	/**
	 * 查询某个手机号 所有主叫类型type=1的通话记录 过滤器
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDB2() throws Exception{

		// 设置过滤器，指定过滤条件，all 即所有过滤条件必须都满足
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

		// 前缀过滤器, 对于rowKey 前面内容必须是18692952699
		PrefixFilter rowFilter = new PrefixFilter("18692952699".getBytes());
		filterList.addFilter(rowFilter);

		// 针对列的value过滤器
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family, "type".getBytes(),
				CompareOp.EQUAL, "1".getBytes());
		filterList.addFilter(singleColumnValueFilter);

		// 添加扫描，设置过滤条件
		Scan scan = new Scan();
		scan.setFilter(filterList);

		// 指定查询范围， 如果不指定，全表扫描，过滤
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String startRow = "18692952699_" + (Long.MAX_VALUE - sdf.parse("20170601000000").getTime());
		String stopRow = "18692952699_" + (Long.MAX_VALUE - sdf.parse("20170501000000").getTime());
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());

		// 指定要扫描的哪些列，这样可以加快查找速度，
		// 也可以只指定查找的哪些列族，
		// 也可以什么都不指定，这样查找每个rowkey下的所有列族下的所有数据
		scan.addColumn(family, "dnum".getBytes());
		scan.addColumn(family, "date".getBytes());
		scan.addColumn(family, "length".getBytes());
		scan.addColumn(family, "type".getBytes());
		
		//对哪个表进行扫描
		ResultScanner scanner = htable.getScanner(scan);
		
		for (Result result : scanner) {
			String dnumValue = new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "dnum".getBytes())));
			String dateValue = new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "date".getBytes())));
			String lengthValue = new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "length".getBytes())));
			String typeValue = new String(CellUtil.cloneValue(result.getColumnLatestCell(family, "type".getBytes())));

			System.out.println("dnum :" + dnumValue + "\tdate :" + dateValue + "\tlength :" + lengthValue + "\ttype :"
					+ typeValue);
		}
	}
	
	/**
	 * 解决hbase k占了大部分资源问题，val占比大大提高，提高了资源利用率 十个用户 每个用户一天产生一百条通话记录
	 * 
	 * @throws Exception
	 */
	@Test
	public void insertDB3() throws Exception {
		List<Put> puts = new ArrayList<Put>();

		for (int i = 0; i < 10; i++) {
			String pnum = htools.getPhoneNum("186");

			Phone.dayPhoneDetail.Builder dayPhoneDetail = Phone.dayPhoneDetail.newBuilder();

			// 一天当中 一百条通话记录内容最后封装到一个对象dayPhoneDetail中
			for (int j = 0; j < 100; j++) {
				String dnum = htools.getPhoneNum("177");
				String datestr = htools.getDate2("20171214");
				String length = r.nextInt(99) + "";
				String type = r.nextInt(2) + "";
				// 将每每天的数据封装到一个对象中
				Phone.phoneDetail.Builder phoneDetail = Phone.phoneDetail.newBuilder();

				phoneDetail.setDnum(dnum);
				phoneDetail.setDate(datestr);
				phoneDetail.setLength(length);
				phoneDetail.setType(type);

				dayPhoneDetail.addPhoneDetails(phoneDetail);
			}

			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			// Rowkey：手机号_（Long.max-通话时间)
			String rowkey = pnum + "_" + (Long.MAX_VALUE - sdf.parse("20171214000000").getTime());

			Put put = new Put(rowkey.getBytes());
			put.add(family, "dayPhone".getBytes(), dayPhoneDetail.build().toByteArray());

			// 将一个用户一天一百条数据内容封装成一个对象dayPhoneDetail，添加到一个put中生成一行数据
			puts.add(put);
		}

		htable.put(puts);
	}
	
	/**
	 * 查询某个手机号 一天产生的一百条通话记录
	 * 
	 * @throws Exception
	 */
	@Test
	public void getDB2() throws Exception {
		String rowkey = "18698125885_9223370523673975807";
		Get get = new Get(rowkey.getBytes());
		// 查询该天18698125885_9223370523673975807所有数据
		get.addColumn("cf".getBytes(), "dayPhone".getBytes());

		Result rs = htable.get(get);
		Cell cell = rs.getColumnLatestCell("cf".getBytes(), "dayPhone".getBytes());

		Phone.dayPhoneDetail dayPhoneDetail = Phone.dayPhoneDetail.parseFrom(CellUtil.cloneValue(cell));

		// 通过api反回每个通话记录对象
		for (Phone.phoneDetail p : dayPhoneDetail.getPhoneDetailsList()) {
			System.out.println(p.getDnum() + " - " + p.getDate() + " - " + p.getType() + " - " + p.getLength());
		}
	}
}
