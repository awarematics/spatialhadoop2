package edu.umn.cs.trajdoop.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joor.Reflect;

import edu.umn.cs.trajdoop.core.schema.STableSchema;
import edu.umn.cs.trajdoop.hcatatlog.mapreduce.PartitionInfo;

public class STable
{

	String tableName = null;
	String alias = null;
	STableSchema schema = null;

	STable[] orgTables = null;
	String restrictOp = null;
	Object[] restrictPreds = null;
	ArrayList<PartitionInfo> partInfos = null;

	public STable()
	{
		schema = new STableSchema();
		partInfos = new ArrayList<PartitionInfo>();
	}

	public STable(String tableName)
	{
		this.tableName = tableName;
		schema = new STableSchema();
		partInfos = new ArrayList<PartitionInfo>();
	}

	public STable(String tableName, String alias)
	{
		this.tableName = tableName;
		this.alias = alias;
		schema = new STableSchema();
		partInfos = new ArrayList<PartitionInfo>();
	}

	public String getTableName()
	{
		return tableName;
	}

	public void addColumn(String name, String type)
	{
		schema.addColumn(name, type);
	}

	public STable select(String column, String predicate, String value)
	{
		STable newTable = new STable();

		newTable.setOrigin(this);
		newTable.setRestrictOp("select");
		newTable.setRestrictPred(new Object[] { "select", column, predicate, value });

		return newTable;
	}

	public STable select(String column, String predicate, Object... values)
	{

		// Reflect.on( schemaMap.get(column) ).call( predicate, values );
		return null;
	}

	public STable project(String[] columns)
	{

		return null;
	}

	public STable join(STable bTable, String aColumn, String bColumn, String predicate)
	{

		return null;
	}

	public STable joinAndProject(STable bTable, String aColumn, String bColumn, String predicate,
			String[] columns)
	{

		return null;
	}

	public STable transform( String transform )
	{
		
		return null;
	}
	
	public STable makeIndex(String indexType, String column)
	{

		return null;
	}

	public boolean setOrigin(STable orgTable)
	{
		this.orgTables = new STable[] { orgTable };
		return true;
	}

	public boolean setOrigin(STable... orgTables)
	{
		this.orgTables = orgTables;
		return true;
	}

	public STable[] getOrigin()
	{
		return this.orgTables;
	}

	public boolean setRestrictOp(String op)
	{
		this.restrictOp = op;

		return true;
	}

	public String getRestrictOp()
	{
		return this.restrictOp;
	}

	public boolean setRestrictPred(Object... restricts)
	{

		this.restrictPreds = restricts;
		return true;
	}

	public Object[] getRestrictPred()
	{
		return this.restrictPreds;
	}

	public int getPartitinSize()
	{
		return partInfos.size();
	}

	public List<PartitionInfo> getPartitions()
	{
		return Collections.unmodifiableList(partInfos);
	}

	public STableSchema getSchema()
	{
		return this.schema;
	}

	public boolean executeQuery()
	{
		return false;
	}
}
