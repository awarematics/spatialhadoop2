package edu.umn.cs.trajdoop.core.schema;

import edu.umn.cs.trajdoop.core.schema.STableSchema.ColumnKind;
import edu.umn.cs.trajdoop.datum.SDataType;


public class SColumn 
{
	String name = null;
	String type = null;
	String comment = null;
	ColumnKind kind = null;
	STableSchema subschema = null; // for next version

	public SColumn( String name, String type, String comment )
	{
		this.name = name;
		this.type = type;
		this.comment = type;
	}
	
	public SColumn( String name, String type, String comment, ColumnKind kind )
	{
		this.name = name;
		this.type = type;
		this.comment = type;
		this.kind = kind;
	}
	
	public String getName()
	{
		return name;
	}
	
	public String getType()
	{
		return type;
	}
	
	public String getComment()
	{
		return comment;
	}
	
	public ColumnKind getColumnKind()
	{
		return kind;
	}
	
	public void setName( String name )
	{
		this.name = name;
	}		
	
	public void setType( String type )
	{
		this.type = type;
	}
	
	@Override
	public String toString()
	{
		return " " + name+":"+type + " ";
	}
}
