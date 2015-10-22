package edu.umn.cs.trajdoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.umn.cs.trajdoop.core.schema.STableSchema;
import edu.umn.cs.trajdoop.hcatatlog.ReaderWriter;

public class SRowPlane implements SRow, Serializable
{
	private final ArrayList<Object> columns;

	public SRowPlane()
	{
		columns = new ArrayList<Object>();
	}

	public SRowPlane(int size)
	{
		columns = new ArrayList<Object>(size);
		for (int i = 0; i < size; i++)
			columns.add(null);
	}

	@Override
	public Object get(int colNum)
	{
		return columns.get(colNum);
	}

	@Override
	public List<Object> getAll()
	{
		return columns;
	}

	@Override
	public void set(int colNum, Object value)
	{
		columns.set(colNum, value);
	}

	@Override
	public int size()
	{
		return columns.size();
	}

	public void remove(int index)
	{
		columns.remove(index);
	}

	void readFields(DataInput in) throws IOException
	{
		columns.clear();
		int len = in.readInt();
		for (int i = 0; i < len; i++)
		{
			columns.add(ReaderWriter.readDatum(in));
		}
	}

	public void write(DataOutput out) throws IOException
	{
		int sz = size();
		out.writeInt(sz);
		for (int i = 0; i < sz; i++)
		{
			ReaderWriter.writeDatum(out, columns.get(i));
		}
	}

	@Override
	public boolean equals(Object other)
	{
		if (other instanceof SRow || other != null)
		{
			SRow otherRow = (SRow) other;

			if (this.size() == otherRow.size())
			{
				int size = this.size();
				for (int i = 0; i < size; i++)
				{
					if (!this.get(i).equals(otherRow.get(i)))
						return false;
				}
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode()
	{
		int hash = 1;
		for (Object o : columns)
		{
			if (o != null)
			{
				hash = 31 * hash + o.hashCode();
			}
		}
		return hash;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (Object o : columns)
		{
			sb.append(o + "\t");
		}
		return sb.toString();
	}

	public Object get(String colName, STableSchema schema)
	{
		return get(schema.getPosition(colName));
	}

	public void set(String colName, STableSchema schema, Object value)
	{
		set(schema.getPosition(colName), value);
	}
	/*
	 * public Boolean getBoolean(String colName, SSchema schema) { return
	 * (Boolean) get( colName, schema, Boolean.class); }
	 * 
	 * public void setBoolean(String colName, SSchema schema, Boolean value ) {
	 * set( colName, schema, value ); }
	 * 
	 * public Byte getByte(String colName, SSchema schema) { // TINYINT return
	 * (Byte) get( colName, schema, Byte.class); }
	 * 
	 * public void setByte(String colName, SSchema schema, Byte value ) { set(
	 * colName, schema, value ); }
	 * 
	 * public Short getShort(String colName, SSchema schema) { // SMALLINT
	 * return (Short) get( colName, schema, Short.class); }
	 * 
	 * public void setShort(String colName, SSchema schema, Short value ) { set(
	 * colName, schema, value ); }
	 * 
	 * public Integer getInteger(String colName, SSchema schema) { return
	 * (Integer) get( colName, schema, Integer.class); }
	 * 
	 * public void setInteger(String colName, SSchema schema, Integer value ) {
	 * set( colName, schema, value ); }
	 * 
	 * public Long getLong(String colName, SSchema schema) { // BIGINT return
	 * (Long) get( colName, schema, Long.class); }
	 * 
	 * public void setLong(String colName, SSchema schema, Long value ) { set(
	 * colName, schema, value ); }
	 * 
	 * public Float getFloat(String colName, SSchema schema) { return (Float)
	 * get( colName, schema, Float.class); }
	 * 
	 * public void setFloat(String colName, SSchema schema, Float value ) { set(
	 * colName, schema, value ); }
	 * 
	 * public Double getDouble(String colName, SSchema schema) { return (Double)
	 * get( colName, schema, Double.class); }
	 * 
	 * public void setDouble(String colName, SSchema schema, Double value ) {
	 * set( colName, schema, value ); }
	 * 
	 * public String getString(String colName, SSchema schema) { return (String)
	 * get( colName, schema, String.class); }
	 * 
	 * public void setString(String colName, SSchema schema, String value ){
	 * set( colName, schema, value ); }
	 * 
	 * @SuppressWarnings("unchecked") public List<? extends Object>
	 * getStruct(String colName, SSchema schema){ return (List<? extends
	 * Object>) get( colName, schema, List.class); }
	 * 
	 * public void setStruct(String colName, SSchema schema, List<? extends
	 * Object> value ){ set( colName, schema, value ); }
	 * 
	 * public List<?> getList(String colName, SSchema schema) { return (List<?>)
	 * get( colName, schema, List.class); }
	 * 
	 * public void setList(String colName, SSchema schema, List<?> value ) {
	 * set( colName, schema, value ); }
	 * 
	 * public Map<?, ?> getMap(String colName, SSchema schema) { return (Map<?,
	 * ?>) get( colName, schema, Map.class); }
	 * 
	 * public void setMap(String colName, SSchema schema, Map<?, ?> value ) {
	 * set( colName, schema, value ); }
	 */
}
