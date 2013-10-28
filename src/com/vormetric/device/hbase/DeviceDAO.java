/**
 * 
 */
package com.vormetric.device.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.vormetric.device.model.DeviceDTO;
import com.vormetric.device.model.DeviceModel;

/**
 * @author xioguo
 *
 */
public class DeviceDAO {
	
	public static final byte[] TABLE_NAME = Bytes.toBytes("devices");
	
	public static final byte[] HASH_FAMILY = Bytes.toBytes("h");
	public static final byte[] HASH_COL = Bytes.toBytes("");
	public static final byte[] LENGTH_FAMILY = Bytes.toBytes("l");  // how many devices merged in this group.
	public static final byte[] LENGTH_COL = Bytes.toBytes("");
	public static final byte[] DEVICE_FAMILY = Bytes.toBytes("d");
	public static final byte[] DEVICE_COL = Bytes.toBytes("");
	public static final byte[] TRANSACTION_FAMILY = Bytes.toBytes("t");
	public static final byte[] TRANSACTION_COL = Bytes.toBytes("");
	
	private static final int longLength = 8;
	
	private HBaseAdmin admin;
	
	private static final Logger log = Logger.getLogger(DeviceDAO.class);
	
	public DeviceDAO (HBaseAdmin admin) {
		this.admin = admin;
	}
	
	public static byte [] mkRowKey(DeviceModel d) {
		return mkRowKey(d.getBrowserHash(), System.currentTimeMillis());
	}
	
	public static byte[] mkRowKey(String hash, long dt) {
		byte[] browserHash = Bytes.toBytes(hash);
		byte[] timestamp = Bytes.toBytes(-1*dt);
		byte[] rowKey = new byte[browserHash.length+timestamp.length];
		
		int offset = 0;
		offset = Bytes.putBytes(rowKey, offset, browserHash, 0, browserHash.length);
		Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);
		return rowKey;
	}
	
	public Put mkPut(DeviceModel d) {
		return mkPut(d, mkRowKey(d), 1, 0, 0);
	}
	
	public Put mkPut(DeviceModel d, byte[] rowKey, int length, int deviceCol, int transactionCol) {
		Put p = new Put(rowKey);
		p.add(HASH_FAMILY, HASH_COL, Bytes.toBytes(d.getBrowserHash()));
		p.add(LENGTH_FAMILY, LENGTH_COL, Bytes.toBytes(length));
		p.add(DEVICE_FAMILY, Bytes.toBytes(deviceCol), Bytes.toBytes(d.getString()));
		p.add(TRANSACTION_FAMILY, Bytes.toBytes(transactionCol), Bytes.toBytes(d.getTransaction()));
		return p;
	}
	
	public Get mkGet(String hash) {
		return null;
	}
	
	
	public List<DeviceDTO> list(String hash) throws IOException {
		List<DeviceDTO> results = new ArrayList<DeviceDTO>();
		HTable t = new HTable(admin.getConfiguration(), TABLE_NAME);
		
		Scan scan = new Scan();
		scan.addColumn(DeviceDAO.HASH_FAMILY, DeviceDAO.HASH_COL);
		scan.addColumn(DeviceDAO.LENGTH_FAMILY, DeviceDAO.LENGTH_COL);
		scan.addFamily(DeviceDAO.DEVICE_FAMILY);
		
		scan.setFilter(new SingleColumnValueFilter(HASH_FAMILY, HASH_COL,
				CompareOp.EQUAL, Bytes.toBytes(hash)));
		
		ResultScanner rs = t.getScanner(scan);
		int i=0;
		for (Result r : rs) {
			results.add(new DeviceDTO(r));
			i++;
		}
		rs.close();
		t.close();
		return results;
	}
}
