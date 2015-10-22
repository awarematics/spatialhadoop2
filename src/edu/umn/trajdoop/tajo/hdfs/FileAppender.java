package edu.umn.trajdoop.tajo.hdfs;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.umn.cs.trajdoop.core.schema.STableSchema;
import edu.umn.cs.trajdoop.tajo.common.Appender;

import java.io.IOException;

public abstract class FileAppender implements Appender
{
	private static final Log LOG = LogFactory.getLog(FileAppender.class);
	protected boolean inited = false;
	protected final Configuration conf;
	protected final STableSchema schema;
	protected final Path workDir;
	protected boolean enabledStats;
	protected Path path;

	public FileAppender(Configuration conf, STableSchema schema, Path workDir)
	{
		this.conf = conf;
		this.schema = schema;
		this.workDir = workDir;
		this.path = workDir;

	}

	public void init() throws IOException
	{
		if (inited)
		{
			throw new IllegalStateException("FileAppender is already initialized.");
		}
		inited = true;
	}

	public void enableStats()
	{
		if (inited)
		{
			throw new IllegalStateException("Should enable this option before init()");
		}
		this.enabledStats = true;
	}

	public long getEstimatedOutputSize() throws IOException
	{
		return getOffset();
	}

	public abstract long getOffset() throws IOException;
}
