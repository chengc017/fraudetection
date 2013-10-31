/**
 * 
 */
package com.vormetric.device.service;

import java.io.IOException;
import java.util.List;

import com.vormetric.device.hbase.DeviceDAO;
import com.vormetric.device.model.DeviceModel;

/**
 * @author xioguo
 *
 */
public class DeviceService {

	private DeviceDAO dao;
	
	public DeviceService(DeviceDAO dao) {
		this.dao = dao;
	}
	
	public List<DeviceModel> getDevicesByBrowserHash(String hash) throws IOException {
		return dao.list(hash);
	}
}
