package edu.rutgers.cs541;

public class ResultInfo {
	private String result;
	private int instanceSize;
	private double elapsedTime;
	
	public ResultInfo(String result, int instanceSize, double elapsedTime)
	{
		this.result = result;
		this.instanceSize = instanceSize;
		this.elapsedTime = elapsedTime;
	}
	
	public String getResult()
	{
		return result;
	}
	
	public int getInstanceSize()
	{
		return instanceSize;
	}
	
	public double getElapsedTime()
	{
		return elapsedTime;
	}
	
	public String toString()
	{
		return "Instance Size: " + instanceSize + ", Completed in " + elapsedTime + "s";
	}
}
