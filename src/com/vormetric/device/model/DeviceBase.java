/**
 * 
 */
package com.vormetric.device.model;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author xioguo
 *
 */
public class DeviceBase extends GenericWritable{

	protected String orgId;
	
	protected String eventId;
	
	protected String requestId;
	
	protected String deviceMatchResult;
	
	protected String sessionId;
	
	protected String browserHash;

	public String getBrowserHash() {
		return browserHash;
	}

	public void setBrowserHash(String browserHash) {
		this.browserHash = browserHash;
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
	
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return null;
	}
	
}
