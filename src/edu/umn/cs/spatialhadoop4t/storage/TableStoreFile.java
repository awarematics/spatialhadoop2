package edu.umn.cs.spatialhadoop4t.storage;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.FileTablespace;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.SeekableScanner;
import org.apache.tajo.storage.TableSpaceManager;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

public class TableStoreFile 
{
	private final Log LOG = LogFactory.getLog( TableStoreFile.class );
	
	FileTablespace fileTablespace = null;
	TajoConf conf = null;
	TableMeta meta = null;
	Schema schema = null;
	Path path = null;
	
	public TableStoreFile( TajoConf conf, TableMeta meta, Schema schema, Path path ) 
			throws IOException
	{
		this.conf = conf;
		fileTablespace = (FileTablespace)TableSpaceManager.getFileStorageManager( conf );
		this.meta = meta;
		this.schema = schema;
		this.path = path;
	}
	
	public SeekableScanner getFileScanner() throws IOException
	{
		return (SeekableScanner)fileTablespace.getFileScanner(meta, schema, path );
	}
	
	public long calculateSize() throws IOException
	{
		return fileTablespace.calculateSize( path );
	}
	
	public long getDefaultBlockSize() throws IOException
	{
		FileSystem fileSystem = path.getFileSystem(conf);
		return fileSystem.getDefaultBlockSize( path );
	}
	
	public Path getPath()
	{
		return path;
	}
}
