/**
 * 
 */
package com.vormetric.device.model;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.vormetric.device.hbase.DeviceDAO;

/**
 * @author xioguo
 *
 */
public class DeviceDTO extends DeviceBase {
	
//	private List<String>
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public DeviceDTO(Result r) {
		this.setBrowserHash(Bytes.toString(r.getColumnLatest(
				DeviceDAO.HASH_FAMILY, DeviceDAO.HASH_COL).getValue()));
//		int length = Bytes.toInt(r.getColumnLatest(DeviceDAO.LENGTH_FAMILY,
//				DeviceDAO.LENGTH_COL).getValue());
		
		NavigableMap<byte[], byte[]> dmap = r.getFamilyMap(DeviceDAO.DEVICE_FAMILY);
		Set<Entry<byte[], byte[]>> entrySet = dmap.entrySet();
		for(Entry<byte[], byte[]> entry: entrySet) {
			System.out.println(Bytes.toInt(entry.getKey()) + ":" + Bytes.toString(entry.getValue()));
		}
	}
}
