package edu.umn.cs.trajdoop.datum;

// This source is borrowed from HCatalog, and modifed.
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

public abstract class SDataType 
{
	public static final byte NULL = 1;
	public static final byte BOOLEAN = 5;
	public static final byte BYTE = 6;
	public static final byte INTEGER = 10;
	public static final byte SHORT = 11;
	public static final byte LONG = 15;
	public static final byte FLOAT = 20;
	public static final byte DOUBLE = 21;
	public static final byte STRING = 23;
	public static final byte MAP = 65;
	public static final byte STRUCT = 66;
	public static final byte LIST = 67;
	
	 // array types
	public static final byte BOOLEAN_ARRAY = 80;
	public static final byte INT1_ARRAY = 81;
	public static final byte INT2_ARRAY = 82;
	public static final byte INT4_ARRAY = 83;
	public static final byte INT8_ARRAY = 84;
	public static final byte UINT1_ARRAY = 85;
	public static final byte UINT2_ARRAY = 86;
	public static final byte UINT4_ARRAY = 87;
	public static final byte UINT8_ARRAY = 88;
	public static final byte FLOAT4_ARRAY = 89;
	public static final byte FLOAT8_ARRAY = 90;
	public static final byte NUMERIC_ARRAY = 91;
	public static final byte CHAR_ARRAY = 92;
	public static final byte NCHAR_ARRAY = 93;
	public static final byte VARCHAR_ARRAY = 94;
	public static final byte NVARCHAR_ARRAY = 95;
	public static final byte TEXT_ARRAY = 96;
	public static final byte DATE_ARRAY = 97;
	public static final byte TIME_ARRAY = 98;
	public static final byte TIMEZ_ARRAY = 99;
	public static final byte TIMESTAMP_ARRAY = 100;
	public static final byte TIMESTAMPZ_ARRAY = 101;
	public static final byte INTERVAL_ARRAY = 102;
	
	public static final byte UDT = 110;
	public static final byte ERROR = -1;
	
	public byte type = 0;
	/**
	 * Determine the datatype of an object.
	 * @param o Object to test.
	 * @return byte code of the type, or ERROR if we don't know.
	 */
	
	public boolean isEqual( SDataType type1, SDataType type2 )
	{
		return type1.type == type2.type;
	}
	
	public static byte findType(Object o) {
		if (o == null) {
			return NULL;
		}
		Class<?> clazz = o.getClass();
		// Try to put the most common first
		if (clazz == String.class) {
			return STRING;
		} else if (clazz == Integer.class) {
			return INTEGER;
		} else if (clazz == Long.class) {
			return LONG;
		} else if (clazz == Float.class) {
			return FLOAT;
		} else if (clazz == Double.class) {
			return DOUBLE;
		} else if (clazz == Boolean.class) {
			return BOOLEAN;
		} else if (clazz == Byte.class) {
			return BYTE;
		} else if (clazz == Short.class) {
			return SHORT;
		} else if (o instanceof List<?>) {
			return LIST;
		} else if (o instanceof Map<?,?>) {
			return MAP;
		} else if (o instanceof UDT ) {
			return UDT;
		} 
		else {return ERROR;}
	}
	public static int compare(Object o1, Object o2) {
		return compare(o1, o2, findType(o1),findType(o2));
	}
	public static int compare(Object o1, Object o2, byte dt1, byte dt2) {
		if (dt1 == dt2) {
			switch (dt1) {
			case NULL:
				return 0;
			case BOOLEAN:
				return ((Boolean)o1).compareTo((Boolean)o2);
			case BYTE:
				return ((Byte)o1).compareTo((Byte)o2);
			case INTEGER:
				return ((Integer)o1).compareTo((Integer)o2);
			case LONG:
				return ((Long)o1).compareTo((Long)o2);
			case FLOAT:
				return ((Float)o1).compareTo((Float)o2);
			case DOUBLE:
				return ((Double)o1).compareTo((Double)o2);
			case STRING:
				return ((String)o1).compareTo((String)o2);
			case SHORT:
				return ((Short)o1).compareTo((Short)o2);
			case LIST:
			{
				List<?> l1 = (List<?>)o1;
				List<?> l2 = (List<?>)o2;
				int len = l1.size();
				if(len != l2.size()) {
					return len - l2.size();
				} else{
					for(int i =0; i < len; i++){
						int cmpVal = compare(l1.get(i), l2.get(i));
						if(cmpVal != 0) {
							return cmpVal;
						}
					}
					return 0;
				}
			}
			case MAP: 
			{
				Map<?,?> m1 = (Map<?,?>)o1;
				Map<?,?> m2 = (Map<?,?>)o2;
				int sz1 = m1.size();
				int sz2 = m2.size();
				if (sz1 < sz2) {
					return -1;
				} else if (sz1 > sz2) {
					return 1;
				} else {
					// This is bad, but we have to sort the keys of the maps in order
					// to be commutative.
					TreeMap<Object,Object> tm1 = new TreeMap<Object,Object>(m1);
					TreeMap<Object, Object> tm2 = new TreeMap<Object,Object>(m2);
					Iterator<Entry<Object, Object>> i1 = tm1.entrySet().iterator();
					Iterator<Entry<Object, Object> > i2 = tm2.entrySet().iterator();
					while (i1.hasNext()) {
						Map.Entry<Object, Object> entry1 = i1.next();
						Map.Entry<Object, Object> entry2 = i2.next();
						int c = compare(entry1.getValue(), entry2.getValue());
						if (c != 0) {
							return c;
						} else {
							c = compare(entry1.getValue(), entry2.getValue());
							if (c != 0) {
								return c;
							}
						}
					}
					return 0;
				}
			}
			
			case UDT :
			{
				
				
			}
			default:
				throw new RuntimeException("Unkown type " + dt1 +
						" in compare");
			}
		} else {
			return dt1 < dt2 ? -1 : 1;
		}
	}
}