package edu.umn.trajdoop.tajo.hdfs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umn.cs.trajdoop.core.schema.SColumn;
import edu.umn.cs.trajdoop.core.schema.STableSchema;
import edu.umn.cs.trajdoop.tajo.common.ColumnStat;
import edu.umn.cs.trajdoop.tajo.common.Fragment;
import edu.umn.cs.trajdoop.tajo.common.Scanner;
import edu.umn.cs.trajdoop.tajo.common.TableStat;

public abstract class FileScanner implements Scanner
{
	private static final Log LOG = LogFactory.getLog(FileScanner.class);
	protected boolean inited = false;
	protected final Configuration conf;
	// protected final TableMeta meta;
	protected final STableSchema schema;
	protected final FileFragment fragment;
	protected final TableStat tableStats;
	protected final int columnNum;
	protected SColumn[] targets;
	protected float progress;

	// protected TableStats tableStats;
	public FileScanner(Configuration conf, final STableSchema schema, final Fragment fragment)
	{
		this.conf = conf;
		this.schema = schema;
		this.fragment = (FileFragment) fragment;
		this.tableStats = new TableStat( schema );
		this.columnNum = this.schema.size();
	}

	public void init() throws IOException
	{
		inited = true;
		progress = 0.0f;
		if (fragment != null)
		{
			tableStats.setNumBytes(fragment.getLength());
			tableStats.setNumBlocks(1);
		}
		
		if (schema != null)
		{
			for (SColumn eachColumn : schema.getColumns())
			{
				ColumnStat columnStats = new ColumnStat(eachColumn);
				tableStats.addColumnStat(columnStats);
			}
		}
	}

	@Override
	public STableSchema getSchema()
	{
		return schema;
	}

	@Override
	public void setTarget(SColumn[] targets)
	{
		if (inited)
		{
			throw new IllegalStateException("Should be called before init()");
		}
		this.targets = targets;
	}

	@Override
	public void setLimit(long num)
	{
	}

	public static FileSystem getFileSystem( Configuration conf, Path path) throws IOException
	{
		FileSystem fs;

		fs = FileSystem.get(path.toUri(), conf);
		return fs;
	}

	@Override
	public float getProgress()
	{
		return progress;
	}
	// @Override
	// public TableStats getInputStats() {
	// return tableStats;
	// }
}
