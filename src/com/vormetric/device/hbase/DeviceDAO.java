/**
 * 
 */
package com.vormetric.device.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

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

import com.vormetric.device.model.DeviceModel;
import com.vormetric.device.utils.AttibutesConvertor;
import com.vormetric.device.utils.Md5Utils;

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
	
	private HBaseAdmin admin;
	
	private static final Logger log = Logger.getLogger(DeviceDAO.class);
	
	public DeviceDAO (HBaseAdmin admin) {
		this.admin = admin;
	}
	
	private static byte [] mkRowKey(DeviceModel d) {
		//create unique row key using request id & timestamp
		return mkRowKey(d.getRequestId(), System.currentTimeMillis());
	}
	
	/*
	 * if you want to create a unique row key on your own, 
	 * pass a string and timemillis.
	 * */
	public static byte[] mkRowKey(String str, long dt) {
		byte[] browserHash = Md5Utils.md5sum(str);
		byte[] timestamp = Bytes.toBytes(-1*dt);
		byte[] rowKey = new byte[browserHash.length+timestamp.length];
		
		int offset = 0;
		offset = Bytes.putBytes(rowKey, offset, browserHash, 0, browserHash.length);
		Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);
		return rowKey;
	}
	
	public Put mkPut(DeviceModel d) {
		//create unique row key using default method
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
		//TODO
		return null;
	}
	
	public List<DeviceModel> list(String hash) throws IOException {
		List<DeviceModel> results = new ArrayList<DeviceModel>();
		HTable t = new HTable(admin.getConfiguration(), TABLE_NAME);
		Scan scan = new Scan();
		scan.addColumn(DeviceDAO.HASH_FAMILY, DeviceDAO.HASH_COL);
		scan.addColumn(DeviceDAO.LENGTH_FAMILY, DeviceDAO.LENGTH_COL);
		scan.addFamily(DeviceDAO.DEVICE_FAMILY);
		scan.setFilter(new SingleColumnValueFilter(HASH_FAMILY, HASH_COL,
				CompareOp.EQUAL, Bytes.toBytes(hash)));
		
		ResultScanner rs = t.getScanner(scan);
		for (Result r : rs) {
			results.addAll(convert(r));
		}
		rs.close();
		t.close();
		return results;
	}
	
	private List<DeviceModel> convert(Result r) {
		List<DeviceModel> deviceList = new ArrayList<DeviceModel> ();
		NavigableMap<byte[], byte[]> dmap = r.getFamilyMap(DeviceDAO.DEVICE_FAMILY);
		Set<Entry<byte[], byte[]>> entrySet = dmap.entrySet();
		for(Entry<byte[], byte[]> entry: entrySet) {
			DeviceModel device = new DeviceModel();
			device.setRowKey(r.getRow());
			device.setBrowserHash(Bytes.toString(r.getColumnLatest(
					DeviceDAO.HASH_FAMILY, DeviceDAO.HASH_COL).getValue()));
			
			AttibutesConvertor convertor = new AttibutesConvertor(
					Bytes.toString(entry.getValue()));
			if(convertor.isValid()) {
				device.setBrowserAttributes(convertor.getBrowserAtts());
				device.setPluginAttributes(convertor.getPluginAtts());
				device.setOsAttributes(convertor.getOSAtts());
				device.setConnectionAttributes(convertor.getConnAtts());
			}
			deviceList.add(device);
		}
		return deviceList;
	}
}
