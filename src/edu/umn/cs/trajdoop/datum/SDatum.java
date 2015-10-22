package edu.umn.cs.trajdoop.datum;

public abstract class SDatum implements Comparable<SDatum>
{

	private final SDataType dataType;

	public SDatum(SDataType DataType)
	{
		this.dataType = DataType;
	}


	public abstract int size();
	public abstract String asChars();
	
	public SDataType getType()
	{
		return this.dataType;
	}

	public boolean isNull()
	{
		return false;
	}

	public boolean isNotNull()
	{
		return true;
	}

	public byte[] asTextBytes()
	{
		return asChars().getBytes();
	}


	public boolean isNumeric()
	{
		return isNumber() || isReal();
	}

	public boolean isNumber()
	{
		return this.dataType.type == SDataType.SHORT || this.dataType.type == SDataType.INTEGER || this.dataType.type == SDataType.FLOAT;
	}

	public boolean isReal()
	{
		return this.dataType.type == SDataType.FLOAT;
	}

	
	@Override
	public String toString()
	{
		return asChars();
	}

}