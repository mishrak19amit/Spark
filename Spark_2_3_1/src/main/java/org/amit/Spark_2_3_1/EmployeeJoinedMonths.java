package org.amit.Spark_2_3_1;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import antlr.StringUtils;
import jodd.util.StringUtil;

public class EmployeeJoinedMonths {

	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("Getting Employee Joined Months").master("local")
				.getOrCreate();
		
		JavaRDD<Employee> employeesflatmap=sc.read().text("/home/moglix/upload/Employee.txt").javaRDD().flatMap(line -> {
			String lines=line.toString();
			String[] parts = lines.split(",");
			List<Employee> employlist=new ArrayList<>();
			
			employlist=getTransformedEmployeeList(parts[0], parts[1], parts[2]);
			
			Employee employee = new Employee();
			employee.setName(parts[0]);
			employee.setMonth("4");
			employee.setYear("2019");
			return employlist.iterator();

		});
		
		System.out.println("Printing values records...");
		
		employeesflatmap.foreach(x-> System.out.println(x.getName()+" "+x.getMonth()+" "+ x.getYear()));
		System.out.println("Printing Done!");

	}

	public static List<Employee> getTransformedEmployeeList(String name, String startdate, String endDate)
	{
		System.out.println(name +" "+ startdate+" "+endDate);
		endDate=endDate.trim();
		name=name.replace("[", "");
		endDate=endDate.trim().replace("]", "");
		List<Employee> employlist=new ArrayList<>();
		int startyear;
		int startmonth;
		int endyear;
		int endmonth;
		
		if(StringUtil.isEmpty(startdate))
			return Collections.emptyList();
		if(StringUtil.isEmpty(endDate))
		{  LocalDate localDate = LocalDate.now();
			endyear=localDate.getYear();
			endmonth=localDate.getMonthValue();
			
		}
		else {
			String[] enddates=endDate.split("-");
			endyear=Integer.parseInt(enddates[0]);
			endmonth=Integer.parseInt(enddates[1]);
		}
		String[] startdates=startdate.split("-");
		startyear=Integer.parseInt(startdates[0]);
		startmonth=Integer.parseInt(startdates[1]);
		
		
		boolean firsttime=true;
		for(int i=startyear;i<=endyear;i++)
		{
			if(i==endyear)
			{
				for(int j=1;j<=endmonth;j++)
				{
					Employee emp= new Employee();
					emp.setName(name);
					emp.setYear(String.valueOf(i));
					emp.setMonth(String.valueOf(j));
					employlist.add(emp);
				}
			}
			else {
				if(firsttime)
				{
					for(int j=startmonth;j<=12;j++)
					{
						Employee emp= new Employee();
						emp.setName(name);
						emp.setYear(String.valueOf(i));
						emp.setMonth(String.valueOf(j));
						employlist.add(emp);
					}
					firsttime=false;
				}
				else {
					for(int j=1;j<=12;j++)
					{
						Employee emp= new Employee();
						emp.setName(name);
						emp.setYear(String.valueOf(i));
						emp.setMonth(String.valueOf(j));
						employlist.add(emp);
					}
					
					
				}
				
			}
		}
			
			
		
		return employlist;
	}
	
	
}
