package edu.umn.cs.trajdoop.tajo.common;

public interface Fragment {
	public abstract String getTableName();
	public abstract long getLength();
	public abstract String getKey();
	public abstract boolean isEmpty();
}