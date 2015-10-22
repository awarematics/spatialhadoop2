package edu.umn.cs.trajdoop.tajo.common;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.umn.cs.trajdoop.core.SRow;
import edu.umn.cs.trajdoop.core.SRowPlane;
import edu.umn.cs.trajdoop.core.schema.STableSchema;

public class TableStat
{
	private static final Log LOG = LogFactory.getLog(TableStat.class);
	private STableSchema schema;
	private SRowPlane minValues;
	private SRowPlane maxValues;
	private long[] numNulls;
	private long numRows = 0;
	private long numBytes = 0;
	private long numBlocks = 0;
	private ArrayList<ColumnStat> columnStats = null;

	public TableStat(STableSchema schema)
	{
		this.schema = schema;
		minValues = new SRowPlane(schema.size());
		maxValues = new SRowPlane(schema.size());
		numNulls = new long[schema.size()];
		columnStats = new ArrayList<ColumnStat>();
	}

	public STableSchema getSchema()
	{
		return this.schema;
	}

	public void incrementRow()
	{
		numRows++;
	}

	public void incrementRows(long num)
	{
		numRows += num;
	}

	public long getNumRows()
	{
		return this.numRows;
	}

	public void setNumBytes(long bytes)
	{
		this.numBytes = bytes;
	}

	public void setNumBlocks(long blocks)
	{
		this.numBlocks = blocks;
	}
	
	public long getNumBytes()
	{
		return this.numBytes;
	}

	public void analyzeNull(int idx)
	{
		numNulls[idx]++;
	}

	public void addColumnStat(ColumnStat columnStat)
	{
		this.columnStats.add( columnStat );
	}

}