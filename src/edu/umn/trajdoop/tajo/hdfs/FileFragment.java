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
import com.google.common.base.Objects;
import edu.umn.cs.trajdoop.tajo.common.Fragment;
import edu.umn.cs.trajdoop.tajo.common.TUtil;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileFragment implements Fragment, Comparable<FileFragment>, Cloneable
{
	private String tableName; // required
	private Path uri; // required
	public Long startOffset; // required
	public Long length; // required

	public FileFragment(String tableName, Path uri, BlockLocation blockLocation) throws IOException
	{
		this.set(tableName, uri, blockLocation.getOffset(), blockLocation.getLength());
	}

	public FileFragment(String tableName, Path uri, long start, long length)
	{
		this.set(tableName, uri, start, length);
	}

	private void set(String tableName, Path path, long start, long length)
	{
		this.tableName = tableName;
		this.uri = path;
		this.startOffset = start;
		this.length = length;
	}

	@Override
	public String getTableName()
	{
		return this.tableName;
	}

	public Path getPath()
	{
		return this.uri;
	}

	public void setPath(Path path)
	{
		this.uri = path;
	}

	public Long getStartKey()
	{
		return this.startOffset;
	}

	@Override
	public String getKey()
	{
		return this.uri.toString();
	}

	@Override
	public long getLength()
	{
		return this.length;
	}

	@Override
	public boolean isEmpty()
	{
		return this.length <= 0;
	}

	/**
	 *
	 * The offset range of tablets <b>MUST NOT</b> be overlapped.
	 *
	 * @param t
	 * @return If the table paths are not same, return -1.
	 */
	@Override
	public int compareTo(FileFragment t)
	{
		if (getPath().equals(t.getPath()))
		{
			long diff = this.getStartKey() - t.getStartKey();
			if (diff < 0)
			{
				return -1;
			} else if (diff > 0)
			{
				return 1;
			} else
			{
				return 0;
			}
		} else
		{
			return -1;
		}
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof FileFragment)
		{
			FileFragment t = (FileFragment) o;
			if (getPath().equals(t.getPath())
					&& TUtil.checkEquals(t.getStartKey(), this.getStartKey())
					&& TUtil.checkEquals(t.getLength(), this.getLength()))
			{
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode()
	{
		return Objects.hashCode(tableName, uri, startOffset, length);
	}

	public Object clone() throws CloneNotSupportedException
	{
		FileFragment frag = (FileFragment) super.clone();
		frag.tableName = tableName;
		frag.uri = uri;
		return frag;
	}

	@Override
	public String toString()
	{
		return "\"fragment\": {\"id\": \"" + tableName + "\", \"path\": " + getPath()
				+ "\", \"start\": " + this.getStartKey() + ",\"length\": " + getLength() + "}";
	}
}