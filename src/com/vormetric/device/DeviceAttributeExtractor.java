/**
 * 
 */
package com.vormetric.device;

import java.util.List;

import org.apache.hadoop.io.Text;

import com.vormetric.device.proto.DeviceProto;
import com.vormetric.device.proto.DeviceProto.Device;
import com.vormetric.device.proto.DeviceProto.Device.Browser;
import com.vormetric.device.proto.DeviceProto.Device.Connection;
import com.vormetric.device.proto.DeviceProto.Device.OS;
import com.vormetric.device.proto.DeviceProto.Device.Plugin;

/**
 * @author xioguo
 *
 */
public class DeviceAttributeExtractor {

	public static final DeviceAttributeExtractor instance = new DeviceAttributeExtractor();
	
	private DeviceAttributeExtractor() {
		
	}
	
	public static DeviceAttributeExtractor getInstance() {
		return DeviceAttributeExtractor.instance;
	}
	
	public Device extract (List<Text> values) {
		Device.Builder device = Device.newBuilder();
		
		device.setOrgId(values.get(1).toString().equals("") ? null : values.get(1).toString());
		device.setEventId(values.get(2).toString().equals("") ? null : values.get(2).toString());
		device.setRequestId(values.get(7).toString().equals("") ? null : values.get(7).toString());
		device.setDeviceMatchResult(values.get(26).toString().equals("") ? null : values.get(26).toString());
		device.setSessionId(values.get(67).toString().equals("") ? null : values.get(67).toString());
		
		Browser.Builder browser = Browser.newBuilder();
		browser.setBrowserLanguage(values.get(12).toString().equals("") ? null : values.get(12).toString());
		browser.setBrowserStringHash(values.get(13).toString().equals("") ? null : values.get(13).toString());
		browser.setBrowserString(values.get(14).toString().equals("") ? null : values.get(14).toString());
		browser.setCssImageLoaded(values.get(16).toString().equals("") ? null : values.get(16).toString());
		browser.setDetectedFl(values.get(11).toString().equals("") ? null : values.get(11).toString());
		browser.setEnabledCk(values.get(30).toString().equals("") ? null : values.get(30).toString());
		browser.setEnabledFl(values.get(31).toString().equals("") ? null : values.get(31).toString());
		browser.setEnabledIm(values.get(32).toString().equals("") ? null : values.get(32).toString());
		browser.setEnabledJs(values.get(33).toString().equals("") ? null : values.get(33).toString());
		browser.setFlashGuid(values.get(36).toString().equals("") ? null : values.get(36).toString());
		browser.setFlashLang(values.get(37).toString().equals("") ? null : values.get(37).toString());
		browser.setFlashOs(values.get(38).toString().equals("") ? null : values.get(38).toString());
		browser.setFlashVersion(values.get(42).toString().equals("") ? null : values.get(42).toString());
		browser.setImageLoaded(values.get(50).toString().equals("") ? null : values.get(50).toString());
		device.setBrowser(browser);
		
		Plugin.Builder plugin = Plugin.newBuilder();
		plugin.setPluginAdobeAcrobat(values.get(57).toString().equals("") ? null : values.get(57).toString());
		plugin.setPluginFlash(values.get(58).toString().equals("") ? null : values.get(58).toString());
		plugin.setPluginHash(values.get(59).toString().equals("") ? null : values.get(59).toString());
		plugin.setPluginSilverlight(values.get(62).toString().equals("") ? null : values.get(62).toString());
		plugin.setPluginNumber(values.get(110).toString().equals("") ? null : values.get(110).toString());
		plugin.setPluginQuicktime(values.get(111).toString().equals("") ? null : values.get(111).toString());
		plugin.setPluginJava(values.get(112).toString().equals("") ? null : values.get(112).toString());
		plugin.setPluginVlcPlayer(values.get(113).toString().equals("") ? null : values.get(113).toString());
		device.setPlugin(plugin);
		
		OS.Builder os = OS.newBuilder();
		os.setOs(values.get(54).toString().equals("") ? null : values.get(54).toString());
		os.setOsFontsHash(values.get(55).toString().equals("") ? null : values.get(55).toString());
		os.setOsFontsNumber(values.get(56).toString().equals("") ? null : values.get(56).toString());
		os.setScreenRes(values.get(65).toString().equals("") ? null : values.get(65).toString());
		os.setTimeZone(values.get(74).toString().equals("") ? null : values.get(74).toString());
		os.setUaBrowser(values.get(91).toString().equals("") ? null : values.get(91).toString());
		os.setUaMobile(values.get(92).toString().equals("") ? null : values.get(92).toString());
		os.setUaOs(values.get(93).toString().equals("") ? null : values.get(93).toString());
		os.setUaPlatform(values.get(94).toString().equals("") ? null : values.get(94).toString());
		os.setTimeZoneDstOffset(values.get(99).toString().equals("") ? null : values.get(99).toString());
		device.setOs(os);
		
		Connection.Builder connection = Connection.newBuilder();
		connection.setTrueIpCity(values.get(150).toString().equals("") ? null : values.get(150).toString());
		connection.setTrueIpGeo(values.get(152).toString().equals("") ? null : values.get(152).toString());
		connection.setTrueIp(values.get(153).toString().equals("") ? null : values.get(153).toString());
		connection.setTrueIpIsp(values.get(154).toString().equals("") ? null : values.get(154).toString());
		connection.setTrueIpRegion(values.get(161).toString().equals("") ? null : values.get(161).toString());
		connection.setProfilingSiteId(values.get(396).toString().equals("") ? null : values.get(396).toString());
		connection.setTcpConnectionType(values.get(445).toString().equals("") ? null : values.get(445).toString());
		connection.setHttpConnectionType(values.get(446).toString().equals("") ? null : values.get(446).toString());
		connection.setScreenResAlt(values.get(449).toString().equals("") ? null : values.get(449).toString());
		//os
		connection.setHttpOsSignature(values.get(45).toString().equals("") ? null : values.get(45).toString());
		connection.setHttpOsSigRaw(values.get(46).toString().equals("") ? null : values.get(46).toString());
		connection.setTcpOsSignature(values.get(70).toString().equals("") ? null : values.get(70).toString());
		connection.setTcpOsSigRaw(values.get(71).toString().equals("") ? null : values.get(71).toString());
		device.setConnection(connection);
		
		return device.build();
	}
}
