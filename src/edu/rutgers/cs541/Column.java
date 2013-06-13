package edu.rutgers.cs541;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Column {
	private String columnName;
	private int dataType;
	private boolean isNullable;
	private int charMaxLength;
	private boolean isPrimaryKey;
	private boolean isUnique;
	protected final ConcurrentMap<Integer, String> values;
	
	public Column(String columnName, int dataType, boolean isNullable, int charMaxLength)
	{
		this.columnName = columnName;
		this.dataType = dataType;
		this.isNullable = isNullable;
		this.charMaxLength = charMaxLength;
		values = new ConcurrentHashMap<Integer, String>();
	}
	
	public String getColumnName()
	{
		return this.columnName;
	}
	
	public int getDataType()
	{
		return this.dataType;
	}
	
	public boolean isNullable()
	{
		return this.isNullable;
	}
	
	public int getCharMaxLength()
	{
		return this.charMaxLength;
	}
	
	public void setPrimaryKey(boolean isPrimaryKey)
	{
		this.isPrimaryKey = isPrimaryKey;
	}
	
	public void setUnique(boolean isUnique)
	{
		this.isUnique = isUnique;
	}
	
	public boolean isPrimaryKey()
	{
		return isPrimaryKey;
	}
	
	public boolean isUnique()
	{
		return isUnique;
	}
	
	public synchronized boolean checkHashMap(String value)
	{
		if(values.containsValue(value))
		{
			return true;
		}
		return false;
	}
	
	public synchronized void insertIntoHashMap(int key, String value)
	{
		values.put(key, value);
	}
	
	public synchronized void clearHashMap()
	{
		values.clear();
	}
}
