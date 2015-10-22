package edu.umn.cs.trajdoop.core;

import java.util.List;

public interface SRow
{

	Object get(int attrNum);

	List<Object> getAll();

	void set(int attrNum, Object value);

	int size();

}
