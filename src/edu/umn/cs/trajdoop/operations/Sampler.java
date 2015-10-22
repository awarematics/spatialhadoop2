/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which 
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/
package edu.umn.cs.trajdoop.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.common.type.TajoTypeUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.util.CommonTestingUtil;

import com.google.common.base.Optional;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeLineRecordReader;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.trajdoop.storage.TableStoreFile;
import edu.umn.cs.trajdoop.tajo.common.Scanner;
import edu.umn.trajdoop.tajo.hdfs.FileTablespace;
import edu.umn.trajdoop.tajo.hdfs.SeekableScanner;
import edu.umn.trajdoop.tajo.hdfs.StorageUtil;
import edu.umn.trajdoop.tajo.hdfs.Tablespace;
import edu.umn.trajdoop.tajo.hdfs.TablespaceManager;
import edu.umn.trajdoop.tajo.hdfs.Tuple;
import edu.umn.trajdoop.tajo.hdfs.fragment.FileFragment;
import edu.umn.trajdoop.tajo.hdfs.fragment.Fragment;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Reads a random sample of a file.
 * @author Kwang Woo Nam
 * 
 */
public class Sampler {
	
	private static final Log LOG = LogFactory.getLog(Sampler.class);
	private TajoConf conf = null;
	private Optional<Tablespace> optTableSpace = null;
	private Path path = null;
	private TableMeta meta = null;
	private Schema schema = null;
	private int geoAttr = 0;
	private String orgType = null;
	private String targetType = null;	 
	
	public void init( String inputPath, String table, StoreType fileType, int numOfAttrs, int geoAttr, 
			String geoType, String targetType ) 
			throws IOException
	{
		// Origin geo attribute/type and Target type
		this.geoAttr = geoAttr;
		this.orgType = geoType;
		this.targetType = targetType;
		
		this.meta = CatalogUtil.newTableMeta( fileType );
		
		String[] names = makeNames( numOfAttrs );
		String[] types = makeTypes( numOfAttrs, geoAttr, geoType );
		this.schema = makeSchema( names, types );		
	
		conf = new TajoConf();
		optTableSpace = TablespaceManager.get( table );
		path = CommonTestingUtil.getTestDir(inputPath);
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
	
/*	public Point[] samplingPointsLocal( int attrNum, int count ) throws IOException
	{
		ArrayList<Envelope> sampled = null;
		Point[] points = null;
		
		sampled = samplingLocal( attrNum, count );
		points = new Point[ sampled.size() ];
		
		Envelope env = null;
		Coordinate coord = null;
		for( int i = 0; i < points.length; i++ )
		{
			env = sampled.get( i );
			coord = env.centre();
			points[i] = new Point( coord.x, coord.y );
		}
		
		return points;
	}
	
	public ArrayList<Envelope> samplingLocal( int count, int attrNum ) throws IOException
	{
		SeekableScanner scanner = null;
		final ArrayList<Datum> sampledResult = new ArrayList<Datum>();
		ArrayList<Envelope> result = null;
		long fileSize = 0;

		scanner = (SeekableScanner)tableSpace.getFileScanner( this.meta, this.schema, this.path );
		fileSize = tableSpace.calculateSize( path );
		
		samplingAttrLocal( scanner, (long)fileSize, count, attrNum, 
				new ResultCollector<Datum>() 
				{
					@Override
					public void collect( Datum d ) 
					{
						sampledResult.add( d );
					}
				}
		);	
		
		result = convertToMBR( sampledResult );
		
		return result;				
	}*/
	
	public Point[] samplingPointsLocal( TableStoreFile tableStore, int attrNum, int count ) throws IOException
	{
		ArrayList<Envelope> sampled = null;
		Point[] points = null;
		
		sampled = samplingLocal( tableStore, attrNum, count );
		points = new Point[ sampled.size() ];
		
		Envelope env = null;
		Coordinate coord = null;
		for( int i = 0; i < points.length; i++ )
		{
			env = sampled.get( i );
			coord = env.centre();
			points[i] = new Point( coord.x, coord.y );
		}
		
		return points;
	}
	
	public ArrayList<Envelope> samplingLocal( TableStoreFile tableStore, int attrNum, int count ) throws IOException
	{
		SeekableScanner scanner = null;
		final ArrayList<Datum> sampledResult = new ArrayList<Datum>();
		ArrayList<Envelope> result = null;
		long fileSize = 0;
		
		scanner = tableStore.getFileScanner();
		fileSize = tableStore.calculateSize();
		
		samplingAttrLocal( scanner, (long)fileSize, count, attrNum, 
				new ResultCollector<Datum>() 
				{
					@Override
					public void collect( Datum d ) 
					{
						sampledResult.add( d );
					}
				}
		);	
		
		result = convertToMBR( sampledResult );
	
		return result;	
	}
	
	public ArrayList<Envelope> convertToMBR( ArrayList<Datum> samples ) 
	{
		ArrayList<Envelope> result = new ArrayList<Envelope>();
		WKTReader wktReader = new WKTReader();
		Geometry orgGeo = null;
		Envelope mbr = null;
		
		try {
			for( Datum d : samples )
			{
				orgGeo = wktReader.read( d.asChars() );	
				mbr = orgGeo.getEnvelopeInternal();
				result.add( mbr );
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	public int samplingAttrLocal( 
			SeekableScanner scanner, long fileSize, int count, int attrNum, ResultCollector<Datum> output ) throws IOException
	{
		long[] offsets = null;
		Tuple tempTuple = null;
		Tuple lastTuple = null;
		int i = 0;

		// Make extra number of random offsets considering failure cases(eg. duplicate or last tuple)
		// This suppose that the failures < 10%
		offsets = getRandomOffsets( fileSize, (int)(count+count*0.1) );

		for( i = 0; i < count; i++ )
		{	
			scanner.seek( offsets[i] );

			tempTuple = scanner.next();

			if ( tempTuple == null )
			{
				--i;
				break;
			}

			if ( lastTuple == null )
				lastTuple = tempTuple;
			else if ( lastTuple.equals( tempTuple ) )
			{
				--i;
				break;
			}
						
			output.collect( tempTuple.getValues()[attrNum-1] );
		}
		
		return i;
	}

	protected long[] getRandomOffsets( long fileSize, int count )
	{
		Random random = new Random( System.currentTimeMillis() );
		long[] offsets = new long[ count ];
		for (int i = 0; i < count ; i++) {
			if (fileSize == 0)
				offsets[i] = 0;
			else
				offsets[i] = Math.abs(random.nextLong()) % fileSize;
		}
		Arrays.sort(offsets);

		return offsets;
	}

	private static void printUsage() {
		System.out.println("Reads a random sample of an input file. Sample is written to stdout");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("fileType:string- Type of the file( csv, ...)");
		System.out.println("numOfAttrs:int - Total number of attributes in the table/file");
		System.out.println("geoAttr:int - Input spatial attribute[1-]");
		System.out.println("geoAttrType:string - Type of Input spatial attribute");
		System.out.println("sampleCount:int - Number of sampled tuples");
		System.out.println("sampleType:string - Target spatial type(point/mbr)");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}


	public static void main(String[] args) throws Exception 
	{
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] inputFiles = params.getPaths();

		// example : java SamplingTest path:/test/tweets.csv fileType:csv numOfAttrs:3 geoAttr:2 geoAttrType:point
		//                     sampleCount:50 sampleType:point
		
		if (!params.checkInput()) {
			printUsage();
			System.exit(1);
		}
		
		Sampler sTest = new Sampler();
		
		//init( String fileType, int numOfAttrs, int geoAttr, String geoType, String targetType )

		sTest.init( params.get("path"),
				"testTable",
				StoreType.CSV, 
				Integer.parseInt( params.get("numOfAttrs")),
				Integer.parseInt( params.get("geoAttr")),
				params.get("geoType"),
				params.get("targetType"));
		
		
		ResultCollector<TextSerializable> output =
				new ResultCollector<TextSerializable>() {
			@Override
			public void collect(TextSerializable value) {
				System.out.println(value.toText(new Text()));
			}
		};
	}

}
