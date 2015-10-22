package edu.umn.cs.trajdoop.hcatatlog;


// This source was borrowed from HCatalog, and modified.
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;

import edu.umn.cs.trajdoop.core.schema.STypeManager;
import edu.umn.cs.trajdoop.datum.SDataType;

public abstract class ReaderWriter {
	private static final String UTF8 = "UTF-8";

	public static Object readDatum(DataInput in) throws IOException {
		byte type = in.readByte();
		switch (type) {
		case SDataType.INTEGER:
			VIntWritable vint = new VIntWritable();
			vint.readFields(in);
			return vint.get();
		case SDataType.LONG:
			VLongWritable vlong = new VLongWritable();
			vlong.readFields(in);
			return vlong.get();
		case SDataType.FLOAT:
			return in.readFloat();
		case SDataType.DOUBLE:
			return in.readDouble();
		case SDataType.BOOLEAN:
			return in.readBoolean();
		case SDataType.BYTE:
			return in.readByte();
		case SDataType.SHORT:
			return in.readShort();
		case SDataType.NULL:
			return null;
		case SDataType.STRING:
			byte[] buffer = new byte[in.readInt()];
			in.readFully(buffer);
			return new String(buffer,UTF8);
		case SDataType.MAP:
			int size = in.readInt();
			Map<Object,Object> m = new HashMap<Object, Object>(size);
			for (int i = 0; i < size; i++) {
				m.put(readDatum(in), readDatum(in));
			}
			return m;
		case SDataType.LIST:
			int sz = in.readInt();
			List<Object> list = new ArrayList<Object>(sz);
			for(int i=0; i < sz; i++) 
			{
				list.add(readDatum(in));
			}
			return list;
		case SDataType.UDT:
			
			
			
			
		default:
			throw new IOException("Unexpected data type " + type +
					" found in stream.");
		}
	}
	public static void writeDatum(DataOutput out, Object val) throws IOException {
		// write the data type
		byte type = SDataType.findType(val);
		switch (type) {
		case SDataType.LIST:
			out.writeByte(SDataType.LIST);
			List<?> list = (List<?>)val;
			int sz = list.size();
			out.writeInt(sz);
			for (int i = 0; i < sz; i++) {
				writeDatum(out, list.get(i));
			}
			return;
		case SDataType.MAP:
			out.writeByte(SDataType.MAP);
			Map<?,?> m = (Map<?, ?>)val;
			out.writeInt(m.size());
			Iterator<?> i =
					m.entrySet().iterator();
			while (i.hasNext()) {
				Entry<?,?> entry = (Entry<?, ?>) i.next();
				writeDatum(out, entry.getKey());
				writeDatum(out, entry.getValue());
			}
			return;
		case SDataType.INTEGER:
			out.writeByte(SDataType.INTEGER);
			new VIntWritable((Integer)val).write(out);
			return;
		case SDataType.LONG:
			out.writeByte(SDataType.LONG);
			new VLongWritable((Long)val).write(out);
			return;
		case SDataType.FLOAT:
			out.writeByte(SDataType.FLOAT);
			out.writeFloat((Float)val);
			return;
		case SDataType.DOUBLE:
			out.writeByte(SDataType.DOUBLE);
			out.writeDouble((Double)val);
			return;
		case SDataType.BOOLEAN:
			out.writeByte(SDataType.BOOLEAN);
			out.writeBoolean((Boolean)val);
			return;
		case SDataType.BYTE:
			out.writeByte(SDataType.BYTE);
			out.writeByte((Byte)val);
			return;
		case SDataType.SHORT:
			out.writeByte(SDataType.SHORT);
			out.writeShort((Short)val);
			return;
		case SDataType.STRING:
			String s = (String)val;
			byte[] utfBytes = s.getBytes(ReaderWriter.UTF8);
			out.writeByte(SDataType.STRING);
			out.writeInt(utfBytes.length);
			out.write(utfBytes);
			return;
			
		case SDataType.UDT:
			
			
			
			
		case SDataType.NULL:
			out.writeByte(SDataType.NULL);
			return;
		default:
			throw new IOException("Unexpected data type " + type +
					" found in stream.");
		}

	}
}