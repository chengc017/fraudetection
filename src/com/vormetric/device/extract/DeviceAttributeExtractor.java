/**
 * 
 */
package com.vormetric.device.extract;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.ArrayListTextWritable;

import com.vormetric.device.model.DeviceModel;
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
	
	public DeviceModel extractModel(List<Text> values) {
		DeviceModel deviceModel = new DeviceModel();
		
		deviceModel.setOrgId(values.get(1).toString());
		deviceModel.setEventId(values.get(2).toString());
		deviceModel.setRequestId(values.get(7).toString());
		deviceModel.setDeviceMatchResult(values.get(26).toString());
		deviceModel.setSessionId(values.get(67).toString());
		
		List<Text> browserAttributes = new LinkedList<Text> ();
		browserAttributes.add(values.get(12));
		browserAttributes.add(values.get(13));
		browserAttributes.add(values.get(14));
		browserAttributes.add(values.get(16));
		browserAttributes.add(values.get(11));
		browserAttributes.add(values.get(30));
		browserAttributes.add(values.get(31));
		browserAttributes.add(values.get(32));
		browserAttributes.add(values.get(33));
		browserAttributes.add(values.get(36));
		browserAttributes.add(values.get(37));
		browserAttributes.add(values.get(38));
		browserAttributes.add(values.get(42));
		browserAttributes.add(values.get(50));
		deviceModel.setBrowserAttributes(browserAttributes);
		
		List<Text> pluginAttributes = new LinkedList<Text> ();
		pluginAttributes.add(values.get(57));
		pluginAttributes.add(values.get(58));
		pluginAttributes.add(values.get(59));
		pluginAttributes.add(values.get(62));
		pluginAttributes.add(values.get(110));
		pluginAttributes.add(values.get(111));
		pluginAttributes.add(values.get(112));
		pluginAttributes.add(values.get(113));
		deviceModel.setPluginAttributes(pluginAttributes);
		
		List<Text> osAttributes = new LinkedList<Text> ();
		osAttributes.add(values.get(54));
		osAttributes.add(values.get(55));
		osAttributes.add(values.get(56));
		osAttributes.add(values.get(65));
		osAttributes.add(values.get(74));
		osAttributes.add(values.get(91));
		osAttributes.add(values.get(92));
		osAttributes.add(values.get(93));
		osAttributes.add(values.get(94));
		osAttributes.add(values.get(99));
		deviceModel.setOsAttributes(osAttributes);
		
		List<Text> connectionAttributes = new LinkedList<Text> ();
		connectionAttributes.add(values.get(150));
		connectionAttributes.add(values.get(152));
		connectionAttributes.add(values.get(153));
		connectionAttributes.add(values.get(154));
		connectionAttributes.add(values.get(161));
		connectionAttributes.add(values.get(396));
		connectionAttributes.add(values.get(445));
		connectionAttributes.add(values.get(446));
		connectionAttributes.add(values.get(449));
		//os
		connectionAttributes.add(values.get(45));
		connectionAttributes.add(values.get(46));
		connectionAttributes.add(values.get(70));
		connectionAttributes.add(values.get(71));
		deviceModel.setConnectionAttributes(connectionAttributes);
		
		return deviceModel;
	}
	
	public List<Text> extractText(List<Text> values) {
		List<Text> valueList = new ArrayListTextWritable ();
		//transaction info
		valueList.add(0, values.get(1));
		valueList.add(1, values.get(2));
//		valueList.add(values.get(7));
//		valueList.add(values.get(26));
//		valueList.add(values.get(67));
		
		//browser
		valueList.add(values.get(12));
		valueList.add(values.get(13));
		valueList.add(values.get(14));
		valueList.add(values.get(16));
		valueList.add(values.get(11));
		valueList.add(values.get(30));
		valueList.add(values.get(31));
		valueList.add(values.get(32));
		valueList.add(values.get(33));
		valueList.add(values.get(36));
		valueList.add(values.get(37));
		valueList.add(values.get(38));
		valueList.add(values.get(42));
		valueList.add(values.get(50));
		//plugin
		valueList.add(values.get(57));
		valueList.add(values.get(58));
		valueList.add(values.get(59));
		valueList.add(values.get(62));
		valueList.add(values.get(110));
		valueList.add(values.get(111));
		valueList.add(values.get(112));
		valueList.add(values.get(113));
		//os
		valueList.add(values.get(54));
		valueList.add(values.get(55));
		valueList.add(values.get(56));
		valueList.add(values.get(65));
		valueList.add(values.get(74));
		valueList.add(values.get(91));
		valueList.add(values.get(92));
		valueList.add(values.get(93));
		valueList.add(values.get(94));
		valueList.add(values.get(99));
		//connection
		valueList.add(values.get(150));
		valueList.add(values.get(152));
		valueList.add(values.get(153));
		valueList.add(values.get(154));
		valueList.add(values.get(161));
		valueList.add(values.get(396));
		valueList.add(values.get(445));
		valueList.add(values.get(446));
		valueList.add(values.get(449));
		//conn-os
		valueList.add(values.get(45));
		valueList.add(values.get(46));
		valueList.add(values.get(70));
		valueList.add(values.get(71));
		return valueList;
	}
	
	public Device extract (List<Text> values) {
		Device.Builder device = Device.newBuilder();
		
		device.setOrgId(values.get(1).toString());
		device.setEventId(values.get(2).toString());
		device.setRequestId(values.get(7).toString());
		device.setDeviceMatchResult(values.get(26).toString());
		device.setSessionId(values.get(67).toString());
		
		Browser.Builder browser = Browser.newBuilder();
		browser.setBrowserLanguage(values.get(12).toString());
		browser.setBrowserStringHash(values.get(13).toString());
		browser.setBrowserString(values.get(14).toString());
		browser.setCssImageLoaded(values.get(16).toString());
		browser.setDetectedFl(values.get(11).toString());
		browser.setEnabledCk(values.get(30).toString());
		browser.setEnabledFl(values.get(31).toString());
		browser.setEnabledIm(values.get(32).toString());
		browser.setEnabledJs(values.get(33).toString());
		browser.setFlashGuid(values.get(36).toString());
		browser.setFlashLang(values.get(37).toString());
		browser.setFlashOs(values.get(38).toString());
		browser.setFlashVersion(values.get(42).toString());
		browser.setImageLoaded(values.get(50).toString());
		device.setBrowser(browser);
		
		Plugin.Builder plugin = Plugin.newBuilder();
		plugin.setPluginAdobeAcrobat(values.get(57).toString());
		plugin.setPluginFlash(values.get(58).toString());
		plugin.setPluginHash(values.get(59).toString());
		plugin.setPluginSilverlight(values.get(62).toString());
		plugin.setPluginNumber(values.get(110).toString());
		plugin.setPluginQuicktime(values.get(111).toString());
		plugin.setPluginJava(values.get(112).toString());
		plugin.setPluginVlcPlayer(values.get(113).toString());
		device.setPlugin(plugin);
		
		OS.Builder os = OS.newBuilder();
		os.setOs(values.get(54).toString());
		os.setOsFontsHash(values.get(55).toString());
		os.setOsFontsNumber(values.get(56).toString());
		os.setScreenRes(values.get(65).toString());
		os.setTimeZone(values.get(74).toString());
		os.setUaBrowser(values.get(91).toString());
		os.setUaMobile(values.get(92).toString());
		os.setUaOs(values.get(93).toString());
		os.setUaPlatform(values.get(94).toString());
		os.setTimeZoneDstOffset(values.get(99).toString());
		device.setOs(os);
		
		Connection.Builder connection = Connection.newBuilder();
		connection.setTrueIpCity(values.get(150).toString());
		connection.setTrueIpGeo(values.get(152).toString());
		connection.setTrueIp(values.get(153).toString());
		connection.setTrueIpIsp(values.get(154).toString());
		connection.setTrueIpRegion(values.get(161).toString());
		connection.setProfilingSiteId(values.get(396).toString());
		connection.setTcpConnectionType(values.get(445).toString());
		connection.setHttpConnectionType(values.get(446).toString());
		connection.setScreenResAlt(values.get(449).toString());
		//os
		connection.setHttpOsSignature(values.get(45).toString());
		connection.setHttpOsSigRaw(values.get(46).toString());
		connection.setTcpOsSignature(values.get(70).toString());
		connection.setTcpOsSigRaw(values.get(71).toString());
		device.setConnection(connection);
		
		return device.build();
	}
}
