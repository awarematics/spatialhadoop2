package edu.umn.cs.trajdoop.algebra;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import edu.umn.cs.trajdoop.core.STable;
import edu.umn.cs.trajdoop.core.schema.STableSchema;
import edu.umn.cs.trajdoop.operations.Trajdoop;

public class Selection implements Algebra {
	
	STable run( String aTable, String op, String predicate )
	{
		
		return null;
	}
	
	public static void main( String[] args ) throws ParseException
	{
		STable st = Trajdoop.textFile("asdf/dfasd/dsf");
		
		st.addColumn( "id", "java.lang.Integer");
		st.addColumn( "name", "java.lang.String");
		st.addColumn( "geo", "com.vividsolutions.jts.geom.Polygon");
		
		// on("lang.Integer").biggerAndEqualThan(price, 3000)
		// st.attribute("price").call("biggerAndEqualThan", "3000")
		WKTReader wktReader = new WKTReader();
		Geometry geom = wktReader.read("POLYGON((10 10, 10 20, 20 30, 10 0))");
		
		STable rt = st.select( "geo", "overlap", geom );
		
		rt = st.select( "salary", "equals", "3000");

		
		// Selection : No index
		STable bt = st.select( "geo", "overlap", "RECT(0,0,10,10)").project( new String[]{"id"} );		
		STable jt = st.join( bt, "aColumn", "bColumn", "predicate");
		jt.executeQuery();
		
		String indexName = Trajdoop.makeIndex( st, "geo", "rtree" );		
		STable it = Trajdoop.indexFile( indexName );
		STable xt = it.select("geo", "overlap", "RECT(0,0,10,10)");
		STable bxt = st.joinAndProject( it, "oid", "oid", "overlap", null);
		
		bxt.executeQuery();
	}
}
