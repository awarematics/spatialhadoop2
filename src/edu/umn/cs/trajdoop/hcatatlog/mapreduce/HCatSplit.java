package edu.umn.cs.trajdoop.hcatatlog.mapreduce;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import edu.umn.cs.trajdoop.core.schema.STableSchema;
/** The HCatSplit wrapper around the InputSplit returned by the underlying InputFormat */
public class HCatSplit extends InputSplit implements Writable {
	private static final Logger LOG = LoggerFactory.getLogger(HCatSplit.class);
	/** The partition info for the split. */
	private PartitionInfo partitionInfo;
	/** The split returned by the underlying InputFormat split. */
	private InputSplit baseMapRedSplit;
	
	/** The schema for the HowlTable */
	private STableSchema tableSchema;

	/**
	 * Instantiates a new hcat split.
	 */
	public HCatSplit() {
	}
	/**
	 * Instantiates a new hcat split.
	 *
	 * @param partitionInfo the partition info
	 * @param baseMapRedSplit the base mapred split
	 */
	public HCatSplit(PartitionInfo partitionInfo,
			InputSplit baseMapRedSplit,
			STableSchema schema ) {
		this.partitionInfo = partitionInfo;
		// dataSchema can be obtained from partitionInfo.getPartitionSchema()
		this.baseMapRedSplit = baseMapRedSplit;
		this.tableSchema = schema;
	}
	
	/**
	 * Gets the partition info.
	 * @return the partitionInfo
	 */
	public PartitionInfo getPartitionInfo() {
		return partitionInfo;
	}
	/**
	 * Gets the underlying InputSplit.
	 * @return the baseMapRedSplit
	 */
	public InputSplit getBaseSplit() {
		return baseMapRedSplit;
	}
	/**
	 * Gets the data schema.
	 * @return the table schema
	 */
	public STableSchema getDataSchema() {
		return this.partitionInfo.getPartitionSchema();
	}
	/**
	 * Gets the table schema.
	 * @return the table schema
	 */
	/*
	public STableSchema getTableSchema() {
		assert this.partitionInfo.getTableInfo() != null : "TableInfo should have been set at this point.";
		return this.partitionInfo.getTableInfo().getAllColumns();
	}
	*/
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
	 */
	@Override
	public long getLength() {
		try {
			return baseMapRedSplit.getLength();
		} catch (Exception e) {
			LOG.warn("Exception in HCatSplit", e);
		}
		return 0; // we errored
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
	 */
	@Override
	public String[] getLocations() {
		try {
			return baseMapRedSplit.getLocations();
		} catch (Exception e) {
			LOG.warn("Exception in HCatSplit", e);
		}
		return new String[0]; // we errored
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput input) throws IOException {
		String partitionInfoString = WritableUtils.readString(input);
		partitionInfo = (PartitionInfo) HCatUtil.deserialize(partitionInfoString);
		String baseSplitClassName = WritableUtils.readString(input);
		InputSplit split;
		try {
			Class<? extends InputSplit> splitClass =
					(Class<? extends InputSplit>) Class.forName(baseSplitClassName);
			//Class.forName().newInstance() does not work if the underlying
			//InputSplit has package visibility
			Constructor<? extends InputSplit>
			constructor =
			splitClass.getDeclaredConstructor(new Class[]{});
			constructor.setAccessible(true);
			split = constructor.newInstance();
			// read baseSplit from input
			((Writable) split).readFields(input);
			this.baseMapRedSplit = split;
		} catch (Exception e) {
			throw new IOException("Exception from " + baseSplitClassName, e);
		}
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput output) throws IOException {
		String partitionInfoString = HCatUtil.serialize(partitionInfo);
		// write partitionInfo into output
		WritableUtils.writeString(output, partitionInfoString);
		WritableUtils.writeString(output, baseMapRedSplit.getClass().getName());
		Writable baseSplitWritable = (Writable) baseMapRedSplit;
		//write baseSplit into output
		baseSplitWritable.write(output);
	}
}
