/**
 * 
 */
package com.vormetric.mapred.io;

import com.google.protobuf.GeneratedMessage;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

/**
 * @author shawnguo
 *
 */
public class ProtobufDeviceWritable<T extends GeneratedMessage> extends ProtobufWritable<T> {
	public ProtobufDeviceWritable() {
		super(new TypeRef<T>() {});
	}
}
