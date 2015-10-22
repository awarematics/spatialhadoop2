package edu.umn.cs.trajdoop.hcatatlog.mapreduce;


import java.io.Serializable;
import java.util.List;

import edu.umn.cs.trajdoop.core.schema.STableSchema;

/** The class used to serialize and store the information read from the metadata server */
public class JobInfo implements Serializable{
	/** The serialization version */
	private static final long serialVersionUID = 1L;
	/** The db and table names. */
	private final String tableName;
	/** The table schema. */
	private final STableSchema tableSchema;
	/** The list of partitions matching the filter. */
	private final List<PartitionInfo> partitions;
	/**
	 * Instantiates a new howl job info.
	 * @param howlTableInfo
	 * @param tableSchema the table schema
	 * @param partitions the partitions
	 */
	public JobInfo( String tableName, STableSchema tableSchema, List<PartitionInfo> partitions) {
		this.tableName = tableName;
		this.tableSchema = tableSchema;
		this.partitions = partitions;
	}
	/**
	 * Gets the value of dbName
	 * @return the dbName
	 */
	public String getDatabaseName() {
		return tableName;
	}
	/**
	 * Gets the value of tableName
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}
	/**
	 * Gets the value of tableSchema
	 * @return the tableSchema
	 */
	public STableSchema getTableSchema() {
		return tableSchema;
	}
	/**
	 * Gets the value of partitions
	 * @return the partitions
	 */
	public List<PartitionInfo> getPartitions() {
		return partitions;
	}
}