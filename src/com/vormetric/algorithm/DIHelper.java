/**
 * 
 */
package com.vormetric.algorithm;


import java.util.ArrayList;
import java.util.List;

import com.vormetric.device.model.DeviceModel;

/**
 * @author shawnkuo
 *
 */
public class DIHelper {

	public static List<DeviceModel> union(Object a, Object b) {
		List<DeviceModel> dup = null;
		if(a instanceof DeviceModel && b instanceof DeviceModel) {
			dup = union((DeviceModel)a, (DeviceModel)b);
		} else if(a instanceof DeviceModel && b instanceof List<?>) {
			dup = union((DeviceModel)a, (ArrayList<DeviceModel>)b);
		} else if(a instanceof List<?> && b instanceof DeviceModel) {
			dup = union((DeviceModel)b, (ArrayList<DeviceModel>)a);
		} else if (a instanceof List<?> && b instanceof List<?>) {
			dup = union((ArrayList<DeviceModel>)a, (ArrayList<DeviceModel>)b);
		}
		return dup;
	}
	
	protected static List<DeviceModel> union(DeviceModel a, List<DeviceModel> b) {
		List<DeviceModel> dup = new ArrayList<DeviceModel> ();
		dup.add(a);
		dup.addAll(b);
		return dup;
	}
	
	protected static List<DeviceModel> union(DeviceModel a, DeviceModel b) {
		List<DeviceModel> dup = new ArrayList<DeviceModel>();
		dup.add(a);
		dup.add(b);
		return dup;
	}
	
	protected static List<DeviceModel> union(List<DeviceModel> a, List<DeviceModel> b) {
		List<DeviceModel> dup = new ArrayList<DeviceModel> ();
		dup.addAll(a);
		dup.addAll(b);
		return dup;
	}
}
