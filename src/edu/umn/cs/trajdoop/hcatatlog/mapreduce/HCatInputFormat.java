package edu.umn.cs.trajdoop.hcatatlog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.umn.cs.trajdoop.common.TrajdoopConsts;
import edu.umn.cs.trajdoop.core.SRow;
import edu.umn.cs.trajdoop.core.STable;
import edu.umn.cs.trajdoop.core.schema.STableSchema;


/** The InputFormat to use to read data from Howl */
public class HCatInputFormat extends InputFormat<WritableComparable, SRow> {
	/**
	 * Set the input to use for the Job. This queries the metadata server with
	 * the specified partition predicates, gets the matching partitions, puts
	 * the information in the conf object. The inputInfo object is updated with
	 * information needed in the client context
	 * @param job the job object
	 * @param schema the table input info
	 * @throws IOException the exception in communicating with the metadata server
	 */
	public static void setInput(Job job, STable table) throws IOException {
		try {
			List<PartitionInfo> partInfos = table.getPartitions();
			
			JobInfo howlJobInfo = new JobInfo( table.getTableName(), table.getSchema(), partInfos);
			job.getConfiguration().set(	TrajdoopConsts.TRAJDOOP_KEY_JOB_INFO,  HCatUtil.serialize(howlJobInfo) );
			
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * Set the schema for the HowlRecord data returned by HowlInputFormat.
	 * @param job the job object
	 * @param schema the schema to use as the consolidated schema
	 */
	public static void setOutputSchema(Job job, STableSchema schema) throws Exception {
		job.getConfiguration().set(TrajdoopConsts.TRAJDOOP_KEY_OUTPUT_SCHEMA, HCatUtil.serialize(schema));
	}
	/**
	 * Logically split the set of input files for the job. Returns the
	 * underlying InputFormat's splits
	 * @param jobContext the job context object
	 * @return the splits, an HowlInputSplit wrapper over the storage
	 * driver InputSplits
	 * @throws IOException or InterruptedException
	 */
	@Override
	public List<InputSplit> getSplits(JobContext jobContext)
			throws IOException, InterruptedException {
		//Get the job info from the configuration,
		//throws exception if not initialized
		JobInfo jobInfo;
		try {
			jobInfo = getJobInfo(jobContext);
		} catch (Exception e) {
			throw new IOException(e);
		}
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<PartitionInfo> partitionInfoList = jobInfo.getPartitions();
		if(partitionInfoList == null ) {
			//No partitions match the specified partition filter
			return splits;
		}
		
		//For each matching partition, call getSplits on the underlying InputFormat
		for(PartitionInfo partitionInfo : partitionInfoList) {
			Job localJob = new Job( jobContext.getConfiguration() );
			HCatInputStorageDriver storageDriver;
			try {
				storageDriver = getInputDriverInstance(partitionInfo.getInputStorageDriverClass());
			} catch (Exception e) {
				throw new IOException(e);
			}
			//Pass all required information to the storage driver
			initStorageDriver(storageDriver, localJob, partitionInfo, jobInfo.getTableSchema());
			//Get the input format for the storage driver
			InputFormat inputFormat =
					storageDriver.getInputFormat(partitionInfo.getInputStorageDriverProperties());
			//Call getSplit on the storage drivers InputFormat, create an
			//HCatSplit for each underlying split
			List<InputSplit> baseSplits = inputFormat.getSplits(localJob);
			for(InputSplit split : baseSplits) {
				splits.add(new HCatSplit( partitionInfo, split, jobInfo.getTableSchema()));
			}
		}
		return splits;
	}
	/**
	 * Create the RecordReader for the given InputSplit. Returns the underlying
	 * RecordReader if the required operations are supported and schema matches
	 * with HowlTable schema. Returns an HowlRecordReader if operations need to
	 * be implemented in Howl.
	 * @param split the split
	 * @param taskContext the task attempt context
	 * @return the record reader instance, either an HowlRecordReader(later) or
	 * the underlying storage driver's RecordReader
	 * @throws IOException or InterruptedException
	 */
	@Override
	public RecordReader<WritableComparable, SRow	> createRecordReader(InputSplit split,
			TaskAttemptContext taskContext) throws IOException, InterruptedException {
		HCatSplit howlSplit = (HCatSplit) split;
		PartitionInfo partitionInfo = howlSplit.getPartitionInfo();
		//If running through a Pig job, the JobInfo will not be available in the
		//backend process context (since HowlLoader works on a copy of the JobContext and does
		//not call HowlInputFormat.setInput in the backend process).
		//So this function should NOT attempt to read the JobInfo.
		HCatInputStorageDriver storageDriver;
		try {
			storageDriver = getInputDriverInstance(partitionInfo.getInputStorageDriverClass());
		} catch (Exception e) {
			throw new IOException(e);
		}
		//Pass all required information to the storage driver
		initStorageDriver(storageDriver, taskContext, partitionInfo, howlSplit.getDataSchema() );
		//Get the input format for the storage driver
		InputFormat inputFormat =
				storageDriver.getInputFormat(partitionInfo.getInputStorageDriverProperties());
		//Create the underlying input formats record record and an Howl wrapper
		RecordReader recordReader =
				inputFormat.createRecordReader(howlSplit.getBaseSplit(), taskContext);
		return new HCatRecordReader(storageDriver,recordReader);
	}
	
	/**
	 * Gets the HowlTable schema for the table specified in the HowlInputFormat.setInput call
	 * on the specified job context. This information is available only after HowlInputFormat.setInput
	 * has been called for a JobContext.
	 * @param context the context
	 * @return the table schema
	 * @throws Exception if HowlInputFromat.setInput has not been called for the current context
	 */
	public static STableSchema getTableSchema(JobContext context) throws Exception {
		JobInfo jobInfo = getJobInfo(context);
		return jobInfo.getTableSchema();
	}
	/**
	 * Gets the JobInfo object by reading the Configuration and deserializing
	 * the string. If JobInfo is not present in the configuration, throws an
	 * exception since that means HowlInputFormat.setInput has not been called.
	 * @param jobContext the job context
	 * @return the JobInfo object
	 * @throws Exception the exception
	 */
	private static JobInfo getJobInfo(JobContext jobContext) throws Exception {
		String jobString = jobContext.getConfiguration().get(TrajdoopConsts.TRAJDOOP_KEY_JOB_INFO);
		if( jobString == null ) {
			throw new Exception("job information not found in JobContext. HowlInputFormat.setInput() not called?");
		}
		return (JobInfo) HCatUtil.deserialize(jobString);
	}
	/**
	 * Initializes the storage driver instance. Passes on the required
	 * schema information, path info and arguments for the supported
	 * features to the storage driver.
	 * @param storageDriver the storage driver
	 * @param context the job context
	 * @param partitionInfo the partition info
	 * @param tableSchema the table level schema
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void initStorageDriver(HCatInputStorageDriver storageDriver,
			JobContext context, PartitionInfo partitionInfo,
			STableSchema tableSchema) throws IOException {
		storageDriver.setInputPath(context, partitionInfo.getLocation());
		if( partitionInfo.getPartitionSchema() != null ) {
			storageDriver.setOriginalSchema(context, partitionInfo.getPartitionSchema());
		}
		storageDriver.setPartitionValues(context, partitionInfo.getPartitionValues());
		//Set the output schema. Use the schema given by user if set, otherwise use the
		//table level schema
		STableSchema outputSchema = null;
		String outputSchemaString = context.getConfiguration().get(TrajdoopConsts.TRAJDOOP_KEY_OUTPUT_SCHEMA );
		if( outputSchemaString != null ) {
			outputSchema = (STableSchema) HCatUtil.deserialize(outputSchemaString);
		} else {
			outputSchema = tableSchema;
		}
		storageDriver.setOutputSchema(context, outputSchema);
		storageDriver.initialize(context, partitionInfo.getInputStorageDriverProperties());
	}
	/**
	 * Gets the input driver instance.
	 * @param inputStorageDriverClass the input storage driver classname
	 * @return the input driver instance
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private HCatInputStorageDriver getInputDriverInstance(
			String inputStorageDriverClass) throws Exception {
		try {
			Class<? extends HCatInputStorageDriver> driverClass =
					(Class<? extends HCatInputStorageDriver>)
					Class.forName(inputStorageDriverClass);
			return driverClass.newInstance();
		} catch(Exception e) {
			throw new Exception("error creating storage driver " +
					inputStorageDriverClass, e);
		}
	}
	
}