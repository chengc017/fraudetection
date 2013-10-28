/**
 * 
 */
package com.vormetric.device.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.vormetric.mapred.io.ArrayListWritable;

/**
 * @author xioguo
 *
 */
public class DeviceModel extends DeviceBase {
	
	private ArrayListWritable<Text> browserAttributes;
	
	private ArrayListWritable<Text> pluginAttributes;
	
	private ArrayListWritable<Text> osAttributes;
	
	private ArrayListWritable<Text> connectionAttributes;
	
	private static Class [] CLASSES = {
		Text.class,
		ArrayListWritable.class
	};
	
	public DeviceModel () {
		browserAttributes = new ArrayListWritable<Text> (); 
		pluginAttributes = new ArrayListWritable<Text> ();
		osAttributes = new ArrayListWritable<Text> ();
		connectionAttributes = new ArrayListWritable<Text> ();
	}
	
	public DeviceModel(String orgId, String eventId, String requestId,
			String deviceMatchResult, String sessionId) {
		this();
		this.orgId = orgId;
		this.eventId = eventId;
		this.requestId = requestId;
		this.deviceMatchResult = deviceMatchResult;
		this.sessionId = sessionId;
	}
	
	public List<Text> getBrowserAttributes() {
		return browserAttributes;
	}

	public void setBrowserAttributes(List<Text> browserAttributes) {
		this.browserAttributes = new ArrayListWritable<Text>(browserAttributes);
	}

	public List<Text> getPluginAttributes() {
		return pluginAttributes;
	}

	public void setPluginAttributes(List<Text> pluginAttributes) {
		this.pluginAttributes = new ArrayListWritable<Text>(pluginAttributes);
	}

	public List<Text> getOsAttributes() {
		return osAttributes;
	}

	public void setOsAttributes(List<Text> osAttributes) {
		this.osAttributes = new ArrayListWritable<Text>(osAttributes);
	}

	public List<Text> getConnectionAttributes() {
		return connectionAttributes;
	}

	public void setConnectionAttributes(List<Text> connectionAttributes) {
		this.connectionAttributes = new ArrayListWritable<Text>(connectionAttributes);
	}
	
	private List<String> convert(List<Text> list) {
		List<String> strList = new LinkedList<String> ();
		for(Text item:list) {
			strList.add(item.toString());
		}
		return strList;
	}
	
	public List<String> all() {
		List<String> allAtt = new LinkedList<String>();
		allAtt.addAll(convert(browserAttributes.subList(0, browserAttributes.size())));
		allAtt.addAll(convert(pluginAttributes.subList(0, pluginAttributes.size())));
		allAtt.addAll(convert(osAttributes.subList(0, osAttributes.size())));
		allAtt.addAll(convert(connectionAttributes.subList(0, connectionAttributes.size())));
		return allAtt;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		orgId = in.readUTF();
		eventId = in.readUTF();
		requestId = in.readUTF();
		deviceMatchResult = in.readUTF();
		sessionId = in.readUTF();
		browserHash = in.readUTF();
		browserAttributes.readFields(in);
		pluginAttributes.readFields(in);
		osAttributes.readFields(in);
		connectionAttributes.readFields(in);
		return;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orgId);
		out.writeUTF(requestId);
		out.writeUTF(eventId);
		out.writeUTF(deviceMatchResult);
		out.writeUTF(sessionId);
		out.writeUTF(browserHash);
		browserAttributes.write(out);
		pluginAttributes.write(out);
		osAttributes.write(out);
		connectionAttributes.write(out);
		return;
	}
	
	public String getTransaction() {
		return orgId + "," + eventId + "," + requestId + ","
				+ deviceMatchResult + "," + sessionId;
	}
	
	public String getString() {
		String deviceString = getBrowserAttributes().toString() + ","
				  + getPluginAttributes().toString() + "," 
				  + getOsAttributes().toString() + ","
				  + getConnectionAttributes().toString();
		return deviceString;
	}
	
	public int hashCode() {
		int hs = 0;
		if(requestId != null) hs ^= requestId.hashCode();
		hs = hs ^ orgId.hashCode();
		hs ^= eventId.hashCode();
		return hs;
	}
	
	public boolean equals(Object obj) {
		if(obj instanceof DeviceModel) {
			DeviceModel dm = (DeviceModel) obj;
			return this.orgId.equals(dm.orgId) && this.eventId.equals(dm.getEventId()) &&
					this.sessionId.equals(dm.getSessionId()) && this.requestId.equals(dm.getRequestId());
		}
		return false;
	}
	
	public String toString() {
		return orgId + "," + eventId + "," + requestId + ","
				+ deviceMatchResult + all().toString();
	}
	
	@Override
	protected Class<? extends Writable> [] getTypes() {
		return CLASSES;
	}

}
