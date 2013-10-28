/**
 * 
 */
package com.vormetric.device;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import com.vormetric.device.hbase.DeviceDAO;

/**
 * @author xioguo
 *
 */
public class DevicesTool {

	private static final Logger log = Logger.getLogger(DevicesTool.class);
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		DeviceDAO dao = new DeviceDAO(admin);
		dao.list("98c2be676aca885697e96e548b0cfbb7");
	}

}
