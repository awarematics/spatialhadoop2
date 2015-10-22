package edu.umn.cs.trajdoop.tajo.common;

import com.google.common.base.Objects;

import edu.umn.cs.trajdoop.core.schema.SColumn;
import edu.umn.cs.trajdoop.datum.SDatum;

public class ColumnStat implements Cloneable
{
	private SColumn column = null; // required
	private Long numDistVals = null; // optional
	private Long numNulls = null; // optional
	private SDatum minValue = null; // optional
	private SDatum maxValue = null; // optional

	public ColumnStat(SColumn column)
	{
		this.column = column;
		numDistVals = 0l;
		numNulls = 0l;
	}

	public SColumn getColumn()
	{
		return this.column;
	}

	public Long getNumDistValues()
	{
		return this.numDistVals;
	}

	public void setNumDistVals(long numDistVals)
	{
		this.numDistVals = numDistVals;
	}

	public boolean minIsNotSet()
	{
		return minValue == null;
	}

	public SDatum getMinValue()
	{
		return this.minValue;
	}

	public void setMinValue( SDatum minValue)
	{
		this.minValue = minValue;
	}

	public boolean maxIsNotSet()
	{
		return maxValue == null;
	}

	public SDatum getMaxValue()
	{
		return this.maxValue;
	}

	public void setMaxValue(SDatum maxValue)
	{
		this.maxValue = maxValue;
	}

	public Long getNumNulls()
	{
		return this.numNulls;
	}

	public void setNumNulls(long numNulls)
	{
		this.numNulls = numNulls;
	}

	public boolean hasNullValue()
	{
		return numNulls > 0;
	}

	public boolean equals(Object obj)
	{
		if (obj instanceof ColumnStat)
		{
			ColumnStat other = (ColumnStat) obj;
			return getColumn().equals(other.getColumn())
					&& getNumDistValues().equals(other.getNumDistValues())
					&& getNumNulls().equals(other.getNumNulls())
					&& TUtil.checkEquals(getMinValue(), other.getMinValue())
					&& TUtil.checkEquals(getMaxValue(), other.getMaxValue());
		} else
		{
			return false;
		}
	}

	public int hashCode()
	{
		return Objects.hashCode(getNumDistValues(), getNumNulls());
	}

	public Object clone() throws CloneNotSupportedException
	{
		ColumnStat stat = (ColumnStat) super.clone();
		stat.column = this.column;
		stat.numDistVals = numDistVals;
		stat.numNulls = numNulls;
		stat.minValue = minValue;
		stat.maxValue = maxValue;
		return stat;
	}

	public String toString()
	{
		return "ColunStats " + column.toString();
	}

}
