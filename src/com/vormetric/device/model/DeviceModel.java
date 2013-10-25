/**
 * 
 */
package com.vormetric.device.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author xioguo
 *
 */
public class DeviceModel extends GenericWritable {
	
	private String orgId;
	
	private String eventId;
	
	private String requestId;
	
	private String deviceMatchResult;
	
	private String sessionId;
	
	private ArrayWritable browserAttributes;
	
	private ArrayWritable pluginAttributes;
	
	private ArrayWritable osAttributes;
	
	private ArrayWritable connectionAttributes;
	
	private static Class [] CLASSES = {
		Text.class,
		ArrayWritable.class
	};
	
	public DeviceModel () {
		browserAttributes = new ArrayWritable(Text.class);
		pluginAttributes = new ArrayWritable(Text.class);
		osAttributes = new ArrayWritable(Text.class);
		connectionAttributes = new ArrayWritable (Text.class);
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

	public String getOrgId() {
		return orgId;
	}

	public void setOrgId(String orgId) {
		this.orgId = orgId;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getDeviceMatchResult() {
		return deviceMatchResult;
	}

	public void setDeviceMatchResult(String deviceMatchResult) {
		this.deviceMatchResult = deviceMatchResult;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	

	public List<String> getBrowserAttributes() {
		String [] values = browserAttributes.toStrings();
		return Arrays.asList(values);
	}

	public void setBrowserAttributes(List<String> browserAttributes) {
		String [] values = new String [browserAttributes.size()]; 
		this.browserAttributes = new ArrayWritable(browserAttributes.toArray(values));
	}

	public List<String> getPluginAttributes() {
		String [] values = pluginAttributes.toStrings();
		return Arrays.asList(values);
	}

	public void setPluginAttributes(List<String> pluginAttributes) {
		String [] values = new String [pluginAttributes.size()];
		this.pluginAttributes = new ArrayWritable(pluginAttributes.toArray(values));
	}

	public List<String> getOsAttributes() {
		String [] values = osAttributes.toStrings();
		return Arrays.asList(values);
	}

	public void setOsAttributes(List<String> osAttributes) {
		String [] values = new String [osAttributes.size()];
		this.osAttributes = new ArrayWritable(osAttributes.toArray(values));
	}

	public List<String> getConnectionAttributes() {
		String [] values = connectionAttributes.toStrings();
		return Arrays.asList(values);
	}

	public void setConnectionAttributes(List<String> connectionAttributes) {
		String [] values = new String[connectionAttributes.size()];
		this.connectionAttributes = new ArrayWritable(connectionAttributes.toArray(values));
	}
	
	public List<String> all() {
		List<String> allAtt = new LinkedList<String>();
		allAtt.addAll(Arrays.asList(browserAttributes.toStrings()));
		allAtt.addAll(Arrays.asList(pluginAttributes.toStrings()));
		allAtt.addAll(Arrays.asList(osAttributes.toStrings()));
		allAtt.addAll(Arrays.asList(connectionAttributes.toStrings()));
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
		
		int size = in.readInt();
		for(int i=0; i<size; i++) {
			browserAttributes.readFields(in);
		}
		
		size = in.readInt();
		for(int i=0; i<size; i++) {
			pluginAttributes.readFields(in);
		}
		
		size = in.readInt();
		for(int i=0; i<size; i++) {
			osAttributes.readFields(in);
		}
		
		size = in.readInt();
		for(int i=0; i<size; i++) {
			connectionAttributes.readFields(in);
		}
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
		
		browserAttributes.write(out);
		pluginAttributes.write(out);
		osAttributes.write(out);
		connectionAttributes.write(out);
		return;
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
				+ deviceMatchResult + "," + all().toString();
	}
	
	@Override
	protected Class<? extends Writable> [] getTypes() {
		return CLASSES;
	}

}
