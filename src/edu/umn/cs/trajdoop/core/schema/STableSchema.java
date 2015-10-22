package edu.umn.cs.trajdoop.core.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class STableSchema implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	ArrayList<SColumn> columns = new ArrayList<SColumn>();
	Map<String, Integer> columnMap = new HashMap<String,Integer>();
	String dataFormat = null;
	
	public enum ColumnKind {
		PRIMITIVE,
		ARRAY,
		SET,
		SPATIAL,
		STRUCT,
		USERDEFINED
	}
	
	public STableSchema()
	{
		
	}
	
	public STableSchema( final List<SColumn> columns )
	{
		int index = 0;
		for( SColumn column : columns )
		{
			columnMap.put( column.getName(), index );
			this.columns.add( column );
			index++;
		}
	}
	
	public boolean addColumn( String attrName, String type )
	{
		Integer retVal = null;
		
		// Assert: Is the type exist?
		if ( attrName == null || isExistType( type ) != true )
		{
			return false;
		}

		// Assert : Is the attribute name already exist?
		if ( ( retVal = columnMap.get( attrName )) != null )
		{			
			columnMap.put( attrName, retVal );
			return false;
		}
		
		//Make the column
		SColumn aColumn = new SColumn( attrName, type, null );
		columns.add( aColumn );
		columnMap.put( attrName, columns.size() );
		return true;
	}
	
	public List<SColumn> getColumns()
	{
		return Collections.unmodifiableList( columns );
	}
	
	public Integer getPosition( String name )
	{
		return columnMap.get( name );
	}
	
	public String getType( String name )
	{
		SColumn column = columns.get( getPosition( name ) );
		
		return column.getType();
	}
	
	public String getType( int index )
	{
		SColumn column = columns.get( index );
		
		return column.getType();
	}
	
	public SColumn getColumn( int index )
	{
		return columns.get( index );
	}
	
	public int size()
	{
		return columns.size();
	}
	
	// TODO : type checking is needed
	public boolean isExistType( String type )
	{
		return true;
	}
	
	@Override
	public String toString()
	{
		boolean first = true;
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("[");
		for( SColumn column : columns )
		{
			if ( first == true ) 
				first = false;  
			else sb.append(",");
			
			sb.append( column.toString() );			
		}
		sb.append("]");
		
		return sb.toString();
	}
}
