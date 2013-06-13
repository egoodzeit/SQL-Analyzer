package edu.rutgers.cs541;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Table {
	private String tableName;
	private List<Column> columns;
	private Statement stmt;
	
	public Table(String tableName, Statement stmt)
	{
		this.tableName = tableName;
		columns = new ArrayList<Column>();
		this.stmt = stmt;
		generateColumnList();
	}
	
	private void generateColumnList()
	{
		try {
			ResultSet rsCol = stmt.executeQuery(
					"SELECT column_name, data_type, is_nullable, character_maximum_length "+
					"FROM information_schema.columns "+
					"WHERE table_schema = 'PUBLIC' "+
					"  AND table_name = '"+tableName+"'" +
					"ORDER BY ordinal_position");
			while(rsCol.next()) {
				String columnName = rsCol.getString(1);
				int dataType = rsCol.getInt(2);
				boolean isNullable = rsCol.getBoolean(3);
				int charMaxLength = rsCol.getInt(4);
				columns.add(new Column(columnName, dataType, isNullable, charMaxLength));
			}
			rsCol.close();
			
			for(Column column : columns)
			{
				rsCol = stmt.executeQuery(
						"SELECT primary_key, non_unique" +
						" FROM information_schema.indexes" +
						" WHERE table_schema = 'PUBLIC'" +
						" AND table_name = '"+tableName+"'" +
						" AND column_name = '"+column.getColumnName()+"'"
						);
				while(rsCol.next()) {
					boolean isPrimaryKey = rsCol.getBoolean(1);
					boolean isNotUnique = rsCol.getBoolean(2);
					column.setPrimaryKey(isPrimaryKey);
					column.setUnique(!isNotUnique);
				}
				rsCol.close();
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getTableName()
	{
		return this.tableName;
	}
	
	public List<Column> getColumns()
	{
		return this.columns;
	}
}
