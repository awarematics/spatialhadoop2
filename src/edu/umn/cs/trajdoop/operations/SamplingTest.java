package edu.umn.cs.trajdoop.operations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.TypeDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.common.type.TajoTypeUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.CommonTestingUtil;

import com.google.common.base.Optional;

import edu.umn.cs.trajdoop.tajo.common.Scanner;
import edu.umn.trajdoop.tajo.hdfs.FileTablespace;
import edu.umn.trajdoop.tajo.hdfs.StorageUtil;
import edu.umn.trajdoop.tajo.hdfs.TableSpaceManager;
import edu.umn.trajdoop.tajo.hdfs.Tablespace;
import edu.umn.trajdoop.tajo.hdfs.TablespaceManager;


public class SamplingTest {

	private TajoConf conf = null;
	private static final String DEFAULT_DIR = "/test/";
	private Path defaultDir = null;
	private Optional<Tablespace> optTablespace = null;
	private FileSystem localFS = null;
	private Path path = null;
	
	private static final Log LOG = LogFactory.getLog( SamplingTest.class );
	
	public Optional<Tablespace> setup( String dir, String inFile ) throws Exception
	{
		conf = new TajoConf();
		defaultDir = CommonTestingUtil.getTestDir( DEFAULT_DIR );
		path = StorageUtil.concatPath( dir, inFile );
		optTablespace = TablespaceManager.get( dir+inFile );
		localFS = defaultDir.getFileSystem(conf);
		
		return optTablespace;
	}
	
	public static Schema makeSchema( String[] names, String[] types )
	{
		Schema schema = new Schema();
		Type type = null;
		
		for( int i = 0 ; i< names.length; i++ )
		{
			type = Type.valueOf( types[i] );
			if ( TajoTypeUtil.isUserDataType( type ) )
			{
				// do something
			}
			schema.addColumn( names[i], type );
		}
		
		return schema;
	}
	
//	public Scanner getScanner( TableMeta meta, Schema schema ) throws IOException
//	{
//		return optTablespace.getFileScanner( meta, schema, path);
//	}
	
	public static String[] makeNames( int numOfAttr )
	{
		String[] names = new String[ numOfAttr ];
		
		for( int i = 0; i < numOfAttr; i++ )
		{
			names[i] = String.valueOf( i );
		}
		
		return names;
	}
	
	public static String[] makeTypes( int numOfAttr, int geoAttr, String geoTypeName )
	{
		String[] typeNames = new String[ numOfAttr ];
		
		for( int i = 0; i < numOfAttr; i++ )
		{
			if ( i == geoAttr )
			{
				typeNames[i] = geoTypeName;
			}
			else
				typeNames[i] = "String";
		}
		
		return typeNames;
	}
	
	public static final int main( String args[] ) throws Exception
	{
		// parameters : 
		//      java SamplingTest fileType dir in_file NumOfAttr geoAttrNum(0-) GeoType NumOfSampling out_file
		// example : java SamplingTest csv /testdir/ tweets 3 2 point sampledPoints
		//
		SamplingTest sTest = new SamplingTest();
		
		sTest.setup( args[1], args[2] );
		
		TableMeta meta = CatalogUtil.newTableMeta( StoreType.CSV );
		
		String[] names = makeNames( Integer.parseInt(args[3]) );
		String[] types = makeTypes( Integer.parseInt(args[3]), Integer.parseInt(args[4]), args[5] );
		Schema schema = makeSchema( names, types );
		// Scanner scanner = sTest.getScanner( meta, schema );
		
		
		return 0;
	}
}
