package edu.umn.cs.trajdoop.storage;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;

import com.google.common.base.Optional;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

import edu.umn.cs.trajdoop.tajo.common.Scanner;
import edu.umn.trajdoop.tajo.hdfs.FileTablespace;
import edu.umn.trajdoop.tajo.hdfs.SeekableScanner;
import edu.umn.trajdoop.tajo.hdfs.Tablespace;
import edu.umn.trajdoop.tajo.hdfs.TablespaceManager;

public class TableStoreFile 
{
	private final Log LOG = LogFactory.getLog( TableStoreFile.class );
	
	FileTablespace tableSpace = null;
	TajoConf conf = null;
	TableMeta meta = null;
	Schema schema = null;
	Path path = null;
	
	public TableStoreFile( TajoConf conf, TableMeta meta, Schema schema, Path path ) 
			throws IOException
	{
		Optional<Tablespace> optTablespace = null;
		
		this.conf = conf;
		optTablespace = TablespaceManager.get( schema.toString() );
		tableSpace = (FileTablespace)optTablespace.get();
		this.meta = meta;
		this.schema = schema;
		this.path = path;
	}	
	
	public SeekableScanner getFileScanner() throws IOException
	{
		return (SeekableScanner)tableSpace.getFileScanner( meta, schema, path );
	}
	
	public long calculateSize() throws IOException
	{
		return tableSpace.calculateSize( path );
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
	
	public void mrProjection( int[] projectAttrs )
	{
		
	}
	
	public void mrSelection( String condition )
	{
		
	}
	
	public TableStoreFile doMapReduce( Class mapperClass, Class reduceClass )
	{
		TableStoreFile tsf = null;
		
		return tsf;		
	}
}
