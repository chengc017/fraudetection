/**
 * 
 */
package com.vormetric.mapred.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * @author xioguo
 *
 */
public class ArrayListWritable <E extends Writable> extends LinkedList<E> implements Writable{

	/**
	 * serial version uid
	 */
	private static final long serialVersionUID = 7085525046985156770L;

	public ArrayListWritable() {
		super();
	}
	
	public ArrayListWritable(List<E> array) {
		super(array);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.clear();

		int numFields = in.readInt();
		if (numFields == 0)
			return;
		String className = in.readUTF();
		E obj;
		try {
			Class c = Class.forName(className);
			for (int i = 0; i < numFields; i++) {
				obj = (E) c.newInstance();
				obj.readFields(in);
				this.add(obj);
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		if (size() == 0)
			return;
		E obj = get(0);

		out.writeUTF(obj.getClass().getCanonicalName());

		for (int i = 0; i < size(); i++) {
			obj = get(i);
			if (obj == null) {
				throw new IOException("Cannot serialize null fields!");
			}
			obj.write(out);
		}
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < this.size(); i++) {
			if (i != 0)
				sb.append(", ");
			sb.append("[");
			sb.append(this.get(i));
			sb.append("]");
		}

		return sb.toString();
	}

}
