package edu.cmu.bqian.bulkload;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper Class
 */
public class HBaseKVMapper extends
Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	// Set column family name
	final static byte[] COL_FAM = "content".getBytes();
	// Number of fields in text file
	final static int NUM_FIELDS = 2;

	String tableName = "";

	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;

	/** {@inheritDoc} */
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		Configuration c = context.getConfiguration();


		// Get parameters
		tableName = c.get("hbase.table.name");
		// in parameter is now saved: "pass parameter example"
		// ( passed from Driver class )
	}

	/** {@inheritDoc} */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("\t");

		hKey.set(String.format("%s", fields[0]).getBytes("UTF-8"));

		// If field exists

		// Save KeyValue Pair
		kv = new KeyValue(hKey.get(), COL_FAM,
				HColumnEnum.COL_B.getColumnName(), Base64.encodeBase64(fields[1].getBytes("UTF-8")));
		// Write KV to HBase
		context.write(hKey, kv);


		context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);

	}
}
