package edu.rutgers.cs541;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TupleGenerator{
	private Statement stmt;
	private Table table;
	private Random rgen;
	private String characters = "abcdefghijklmnopqrstuvwxyz";
	private List<Column> columns;
	protected final AtomicInteger hashKey = new AtomicInteger();
	protected final ExecutorService workers = Executors.newCachedThreadPool();
	
	public TupleGenerator(Table table)
	{
		this.table = table;
		rgen = new Random();
		columns = table.getColumns();
	}
	
	public void generateTuples(final Statement stmt, int instanceSize)
	{
		for(int i = 0; i < instanceSize; i++)
		{
			if(rgen.nextInt(10) == 0)
			{
				//don't insert any tuples
				return;
			}
			
			final StringBuilder insertSb = new StringBuilder();
			insertSb.append("INSERT INTO ");
			insertSb.append(table.getTableName());
			insertSb.append(" VALUES (");
			int colNum = 1;
			
			for(Column column : columns) {
				String columnName = column.getColumnName();
				int dataType = column.getDataType();
				boolean isNullable = column.isNullable();
				int char_max_length = column.getCharMaxLength();
				boolean isPrimaryKey = column.isPrimaryKey();
				boolean isUnique = column.isUnique();
				
				if(colNum++ != 1) {
					insertSb.append(", ");
				}
				boolean nullProb = rgen.nextInt(10) == 0;
				// generate a value appropriate for the column's type
				switch(dataType) {
				case Types.INTEGER:
					int rInt = rgen.nextInt(instanceSize);
					if(isNullable && nullProb)
					{
						insertSb.append("NULL");
					}
					else
					{
						if(isPrimaryKey || isUnique)
						{
							while(column.checkHashMap("" + rInt))
							{
								rInt = rgen.nextInt(instanceSize);
							}
							insertSb.append("" + rInt);
							column.insertIntoHashMap(hashKey.incrementAndGet(), "" + rInt);
						}
						else
						{
							insertSb.append("" + rInt);
						}
					}	
					break;
				case Types.DOUBLE:
					double rDouble = formatDouble(rgen.nextDouble(), instanceSize);
					if(isNullable && nullProb)
					{
						insertSb.append("NULL");
					}
					else
					{
						if(isPrimaryKey || isUnique)
						{
							while(column.checkHashMap("" + rDouble))
							{
								rDouble = formatDouble(rgen.nextDouble(), instanceSize);
							}
							insertSb.append("" + rDouble);
							column.insertIntoHashMap(hashKey.incrementAndGet(), "" + rDouble);
						}
						else
						{
							insertSb.append("" + rDouble);
						}
					}
					break;
				case Types.VARCHAR:
					if(isNullable && nullProb)
					{
						insertSb.append("NULL");
					}
					else
					{
						if(isPrimaryKey || isUnique)
						{
							String rString = "" + rgen.nextInt(instanceSize);
							while(column.checkHashMap(rString))
							{
								rString = "" + rgen.nextInt(instanceSize);
							} 
							insertSb.append("'"+ rString +"'");
							column.insertIntoHashMap(hashKey.incrementAndGet(), rString);
						}
						else
						{
							insertSb.append("'"+(""+rgen.nextInt(instanceSize))+"'");
						}
					}
					break;
				default:
					System.err.println("Column \""+columnName
							+"\" of Table "+table.getTableName()
							+" has unsupported data type "+dataType);
					//croak;
					System.exit(1);
				}
			}
			insertSb.append(")");
			try {
				stmt.executeUpdate(insertSb.toString());
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		}
		hashKey.set(0);
		for(Column column : columns)
		{
			column.clearHashMap();
		}

	}
	
	private double formatDouble(double d, int instanceSize)
	{
		if(instanceSize >= 10)
		{
			d = (d * instanceSize)/10;
		}
		DecimalFormat dFormat = new DecimalFormat("#.#");
		return Double.valueOf(dFormat.format(d));
	}
	
	private String generateString(int stringLen)
	{
		StringBuilder randString = new StringBuilder();
		for(int i = 0; i < stringLen; i++)
		{
			randString.append(characters.charAt(rgen.nextInt(characters.length())));
		}
		return randString.toString();
	}
	
	public void clearTable(Statement stmt)
	{
		try {
			stmt.executeUpdate("TRUNCATE TABLE " + table.getTableName());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(Column column : columns)
		{
			column.clearHashMap();
		}
		hashKey.set(0);
	}
	


}
