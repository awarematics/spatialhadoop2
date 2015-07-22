/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which 
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/
package edu.umn.cs.spatialhadoop4t.operations;

import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.FileTablespace;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.SeekableScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GridPartitioner;
import edu.umn.cs.spatialHadoop.indexing.HilbertCurvePartitioner;
import edu.umn.cs.spatialHadoop.indexing.IndexOutputFormat;
import edu.umn.cs.spatialHadoop.indexing.IndexOutputFormat.IndexRecordWriter;
import edu.umn.cs.spatialHadoop.indexing.KdTreePartitioner;
import edu.umn.cs.spatialHadoop.indexing.LocalIndexer;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;
import edu.umn.cs.spatialHadoop.indexing.QuadTreePartitioner;
import edu.umn.cs.spatialHadoop.indexing.RTreeLocalIndexer;
import edu.umn.cs.spatialHadoop.indexing.STRPartitioner;
import edu.umn.cs.spatialHadoop.indexing.ZCurvePartitioner;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import edu.umn.cs.spatialhadoop4t.storage.TableStore;
import edu.umn.cs.spatialhadoop4t.storage.TableStoreFile;

/**
 * @author Ahmed Eldawy and Kwang Woo Nam
 *
 */
public class Indexer {
	private static final Log LOG = LogFactory.getLog(Indexer.class);

	private static final Map<String, Class<? extends Partitioner>> PartitionerClasses;
	private static final Map<String, Class<? extends LocalIndexer>> LocalIndexes;
	private static final Map<String, Boolean> PartitionerReplicate;
	
	private static final double INDEXING_OVERHEAD = 0.2;

	static {
		PartitionerClasses = new HashMap<String, Class<? extends Partitioner>>();
		PartitionerClasses.put("grid", GridPartitioner.class);
		PartitionerClasses.put("str", STRPartitioner.class);
		PartitionerClasses.put("str+", STRPartitioner.class);
		PartitionerClasses.put("rtree", STRPartitioner.class);
		PartitionerClasses.put("r+tree", STRPartitioner.class);
		PartitionerClasses.put("quadtree", QuadTreePartitioner.class);
		PartitionerClasses.put("zcurve", ZCurvePartitioner.class);
		PartitionerClasses.put("hilbert", HilbertCurvePartitioner.class);
		PartitionerClasses.put("kdtree", KdTreePartitioner.class);

		PartitionerReplicate = new HashMap<String, Boolean>();
		PartitionerReplicate.put("grid", true);
		PartitionerReplicate.put("str", false);
		PartitionerReplicate.put("str+", true);
		PartitionerReplicate.put("rtree", false);
		PartitionerReplicate.put("r+tree", true);
		PartitionerReplicate.put("quadtree", true);
		PartitionerReplicate.put("zcurve", false);
		PartitionerReplicate.put("hilbert", false);
		PartitionerReplicate.put("kdtree", true);

		LocalIndexes = new HashMap<String, Class<? extends LocalIndexer>>();
		LocalIndexes.put("rtree", RTreeLocalIndexer.class);
		LocalIndexes.put("r+tree", RTreeLocalIndexer.class);
	}

	// @author : Kwang Woo Nam
	// @param type : local or mpareduce
	// @return

	private void indexSpatialLocal( TableStoreFile inTable, int attrNum, Envelope mbr, 
			String indexKind, int samplingCount, TableStoreFile outTable,
			OperationsParams params ) throws IOException, InterruptedException
	{
		Sampler sampler = new Sampler();
		Point[] points = null;
		Partitioner partitioner = null;
		Rectangle inMBR = null;

		// Setting for Table Reader
		IndexTupleIterator reader = makeTableIteratorLocal( inTable, attrNum );
		
		// Sampling for partitions
		points = sampler.samplingPointsLocal ( inTable, attrNum, samplingCount );	  
		
		// Making spatial partitions
		partitioner = createPartitioner( indexKind );
		int samplePerPartition = getSamplePerPartition( inTable, points.length );
		inMBR = new Rectangle( mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY());
		partitioner.createFromPoints( inMBR, points, samplePerPartition );
		
		// Setting for Index Writer 
		Job job = Job.getInstance(params);
		final Configuration conf = job.getConfiguration();
		boolean replicate = PartitionerReplicate.get( indexKind );
		
		this.setLocalIndexer( conf, indexKind );
		final IndexRecordWriter<Shape> writer = new IndexRecordWriter<Shape>(
				partitioner, replicate, indexKind, outTable.getPath(), conf);		

		// Write partition files
		writeIndex( partitioner, replicate, reader, writer );
		
		// Write Index Files
		writer.close(null);
	}
 
	protected IndexTupleIterator makeTableIteratorLocal( TableStoreFile inTable, int attrNum ) 
			throws IOException 
	{
		SeekableScanner scanner = inTable.getFileScanner();
		
		IndexTupleIterator iter = new IndexTupleIterator( scanner, attrNum );
		
		return iter;
	}
	
	public class SpatialIndexTuple implements Shape
	{
		public Rectangle spatialKey;
		public long rowid;
		public SpatialIndexTuple( Rectangle key, long rowid )
		{
			this.spatialKey = key;
			this.rowid = rowid;
		}
		
		@Override
		public void readFields(DataInput in ) throws IOException 
		{
			if ( spatialKey == null )
				spatialKey = new Rectangle();
			
			this.spatialKey.readFields(in);
		    this.rowid = in.readLong();
		}

		@Override
		public void write(DataOutput out ) throws IOException {
			spatialKey.write( out );
			out.writeLong( rowid );
		}

		@Override
		public Text toText(Text text) {
		    TextSerializerHelper.serializeDouble( spatialKey.x1, text, ',');
		    TextSerializerHelper.serializeDouble( spatialKey.y1, text, ',');
		    TextSerializerHelper.serializeDouble( spatialKey.x2, text, ',');
		    TextSerializerHelper.serializeDouble( spatialKey.y2, text, ',');
		    TextSerializerHelper.serializeLong( rowid, text, '\0');

		    return text;
		}

		@Override
		public void fromText(Text text) {
		    spatialKey.x1 = TextSerializerHelper.consumeDouble(text, ',');
		    spatialKey.y1 = TextSerializerHelper.consumeDouble(text, ',');
		    spatialKey.x2 = TextSerializerHelper.consumeDouble(text, ',');
		    spatialKey.y2 = TextSerializerHelper.consumeDouble(text, ',');
		    rowid = TextSerializerHelper.consumeLong(text, '\0');			
		}

		@Override
		public Rectangle getMBR() {
			return spatialKey.getMBR();
		}

		@Override
		public double distanceTo(double x, double y) {
			return spatialKey.distanceTo( x, y );
		}

		@Override
		public boolean isIntersected(Shape s) {
			return spatialKey.isIntersected(s);
		}

		@Override
		public Shape clone() {
			return new SpatialIndexTuple( spatialKey.clone(), rowid );
		}

		@Override
		public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
				int imageHeight, double scale) {
			spatialKey.draw( g, fileMBR, imageWidth, imageHeight, scale );
		}

		@Override
		public void draw(Graphics g, double xscale, double yscale) {
			spatialKey.draw( g, xscale, yscale);
		}		
	}
	
	public class IndexTupleIterator implements Iterator<SpatialIndexTuple>
	{
		Scanner scanner = null;
		int geoAttrNum = 0;
		SpatialIndexTuple nextTuple = null;
		WKTReader wktReader = new WKTReader();

		public IndexTupleIterator( Scanner scanner, int geoAttrNum )
		{
			this.scanner = scanner;
			this.geoAttrNum = geoAttrNum;
		}
		
		@Override
		public boolean hasNext() {
			
			try {
				Tuple tuple = scanner.next();
				
				if ( tuple == null )
					return false;
				
				Datum d = tuple.asDatum( geoAttrNum );
				Geometry geo = wktReader.read( d.asChars() );				
				Envelope mbr = geo.getEnvelopeInternal();
				Rectangle rect = new Rectangle( mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY() );
				
				long rowid = tuple.getOffset();
				
				nextTuple = new SpatialIndexTuple( rect, rowid );

			} catch (IOException e) {
				e.printStackTrace();
				nextTuple = null;
				return false;
			} catch (ParseException e) {
				e.printStackTrace();
				return false;
			}	
			
			return true;
		}

		@Override
		public SpatialIndexTuple next() {
			return nextTuple;
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			// do nothing
		}
		
		public void close()
		{
			try {
				scanner.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			wktReader = null;
		}
	}
	
	private int getSamplePerPartition( TableStoreFile inTable, int sampleCount ) throws IOException
	{
		long inSize = inTable.calculateSize();
		long estimatedOutSize = (long) (inSize * (1.0 + INDEXING_OVERHEAD));
		long outBlockSize = inTable.getDefaultBlockSize();
		int partitionCapacity = (int) Math.max(1, Math.floor( sampleCount * outBlockSize / estimatedOutSize));
		
		return partitionCapacity;
	}	

	public void writeIndex( Partitioner partitioner, boolean replicate,
			IndexTupleIterator reader, final IndexRecordWriter<Shape> recordWriter ) 
					throws IOException, InterruptedException
	{
		final IntWritable partitionID = new IntWritable();

		while ( reader.hasNext() ) {
			final SpatialIndexTuple tuple = reader.next();
			if (replicate) {
					partitioner.overlapPartitions( tuple, new ResultCollector<Integer>() {
						@Override
						public void collect(Integer id) {
							partitionID.set(id);
							try {
								recordWriter.write(partitionID, tuple);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
					});
				
			} else {
					int pid = partitioner.overlapPartition( tuple );
					if (pid != -1) {
						partitionID.set(pid);
						recordWriter.write(partitionID, tuple );
					}
			}
		}
		reader.close();
	}
	
	public Partitioner createPartitioner( String partitionerName ) 
			throws IOException
	{
		Partitioner partitioner = null;
		Class<? extends Partitioner> partitionerClass =
				PartitionerClasses.get(partitionerName.toLowerCase());
		if (partitionerClass == null) {
			// Try to parse the name as a class name
			try {
				partitionerClass = Class.forName(partitionerName).asSubclass(Partitioner.class);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Unknown index type '"+partitionerName+"'");
			}
		}

		try {
			partitioner = partitionerClass.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
			return null;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return null;
		}

		return partitioner;
	}

	/**
	 * Set the local indexer for the given job configuration.
	 * @param job
	 * @param sindex
	 */
	private void setLocalIndexer(Configuration conf, String sindex) {
		Class<? extends LocalIndexer> localIndexerClass = LocalIndexes.get(sindex);
		if (localIndexerClass != null)
			conf.setClass(LocalIndexer.LocalIndexerClass, localIndexerClass, LocalIndexer.class);
	}
	
	// updated : 2015.06.09 Kwang Woo Nam
	//           Added Spatial Column Attribute and Primary Key Column Attribute Options
	protected static void printUsage() {
		System.out.println("Builds a spatial index on an input file");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("<output file> - (*) Path to output file");
		System.out.println("shape:<point|rectangle|polygon> - (*) Type of shapes stored in input file");
		System.out.println("sindex:<index> - (*) Type of spatial index (grid|str|str+|quadtree|zcurve|kdtree)");
		System.out.println("spatialcolumn:i - spatial column is i-th column");
		System.out.println("keycolumn:j - primary key column is j-th column");
		System.out.println("-overwrite - Overwrite output file without noitce");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	/**
	 * Entry point to the indexing operation.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

		if (!params.checkInputOutput(true)) {
			printUsage();
			return;
		}
		if (params.get("sindex") == null) {
			System.err.println("Please specify type of index to build (grid, rtree, r+tree, str, str+)");
			printUsage();
			return;
		}
		Path inputPath = params.getInputPath();
		Path outputPath = params.getOutputPath();

		// The spatial index to use
		long t1 = System.currentTimeMillis();
		// index(inputPath, outputPath, params);
		long t2 = System.currentTimeMillis();
		System.out.println("Total indexing time in millis "+(t2-t1));
	}

}
