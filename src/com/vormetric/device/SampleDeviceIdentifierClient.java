/**
 * 
 */
package com.vormetric.device;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVWriter;

import com.vormetric.algorithm.decision.Match;
import com.vormetric.algorithm.decision.SVMDeviceSimilarityDecision;
import com.vormetric.algorithm.similarities.JaccardCoefficientSimilarity;
import com.vormetric.device.extract.DeviceAttributeExtractor;
import com.vormetric.device.hbase.DeviceDAO;
import com.vormetric.device.model.DeviceModel;
import com.vormetric.device.service.DeviceService;
import com.vormetric.device.utils.ConfigUtil;

/**
 * @author xioguo
 *
 */
public class SampleDeviceIdentifierClient {

	private static final Logger logger = Logger.getLogger(SampleDeviceIdentifierClient.class);
	
	public static final String SEEN_DEVICE_FILE = "./seen_devices.txt";
	public static final String NEW_DEVICE_FILE = "./new_devices.txt";
	
	private static final String useage = "Input csv file.";
	
	private static Map<byte[], List<DeviceModel>> seenDeviceMap = new HashMap<byte[], List<DeviceModel>>();
	private static List<DeviceModel> seenDeviceList = new ArrayList<DeviceModel>();
	private static List<DeviceModel> newDeviceList = new ArrayList<DeviceModel>();
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		if(args.length < 1) {
			System.out.println(useage);
			return;
		}
		String csvFile = args[0];
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", ConfigUtil.getHbaseHost());
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		DeviceDAO dao = new DeviceDAO(admin);
		DeviceService service = new DeviceService(dao);
		
		SVMDeviceSimilarityDecision decision = new SVMDeviceSimilarityDecision(
				new JaccardCoefficientSimilarity());
		
		int intputDeviceNumber = 0;
		int newDeviceNumber = 0;
		int seenDeviceNumber = 0;
		int exactSameSeenDeviceNumber = 0;
		
		logger.info("parse device attributes from input csv file...");
		List<DeviceModel> devices = DeviceAttributeExtractor.getInstance()
				.extract(csvFile);
		intputDeviceNumber = devices.size();
		
		logger.info("Detecting devices...");
		for(DeviceModel device : devices) {
			//query database
			List<DeviceModel> result = service.getDevicesByBrowserHash(device
					.getBrowserHash());
			boolean seen = false;
			for(DeviceModel dm : result) {
				Match match = decision.match(device, dm);
				if(match.result) { // seen
					//prepare for saving to DB. But only save those not exactly same devices.
					if(match.score != 1) {
						List<DeviceModel> value = seenDeviceMap.get(dm.getRowKey());
						if(value == null) {
							value = new ArrayList<DeviceModel>();
							value.add(device);
							seenDeviceMap.put(dm.getRowKey(), value);	
						} else {
							value.add(device);
						}
					}
					seenDeviceList.add(device);
					seen = true;
					break;
				} 
			}
			
			if(!seen) {
				logger.info("New device found.");
				newDeviceList.add(device);
			}
		}
		admin.close();
		
		newDeviceNumber = newDeviceList.size();
		seenDeviceNumber = seenDeviceList.size();
		exactSameSeenDeviceNumber = seenDeviceNumber - seenDeviceMap.size();
		
		//update DB - insert new devices and merge seen devices to existed device row.
		StringBuffer statistics = new StringBuffer("");
		statistics.append("---------------------------------- Statistics -----------------------------------\n")
				  .append("Input devices: " + intputDeviceNumber + "\n")
				  .append("New devices detected: " + newDeviceNumber + "\n")
				  .append("Seen devices detected: " + seenDeviceNumber)
				  .append("\n" + exactSameSeenDeviceNumber)
				  .append(" of which are exactly the same with the existing ones,\n")
				  .append("the rest may have some slight changes but still considered as same(seen) devices.\n")
				  .append("----------------------------------------------------------------------------------");
		System.out.println(statistics.toString());
		//print result on screen.
		Thread.sleep(1000);
		logger.info("Writing seen devices to file - seen_devices.txt...");
		write(seenDeviceList, SEEN_DEVICE_FILE);
		Thread.sleep(1000);
		logger.info("Writing new devices to file - new_devices.txt...");
		write(newDeviceList, NEW_DEVICE_FILE);
		
		System.out.println("Seen/New devices are saved in two seperate files, see details in files.");
		System.out.println("Done.");
	}
	
	private static void write(List<DeviceModel> list, String fileName)
			throws IOException {
		File file = new File(fileName);
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		for (DeviceModel d : list) {
			bw.write(d.toString()+"\n\n");
		}
		bw.close();
	}
}
