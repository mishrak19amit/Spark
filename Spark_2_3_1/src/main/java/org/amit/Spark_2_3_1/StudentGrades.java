package org.amit.Spark_2_3_1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class StudentGrades {
	
	public static void main(String[] args) {
		SparkSession spark= SparkSession.builder().appName("Student_Grads").master("local").getOrCreate();
		
		Dataset<Row> studentdf=spark.read().json("/home/moglix/Desktop/Amit/PGitHub/Spark/data/StudentRecords.json");
		Dataset<Row> gradedf=spark.read().json("/home/moglix/Desktop/Amit/PGitHub/Spark/data/GradeJson.json");
		studentdf.printSchema();
		gradedf.printSchema();
		studentdf.createOrReplaceTempView("Students");
		gradedf.createOrReplaceTempView("Grades");
		System.out.println("============>>");
		Dataset<Row> gretermarkdf=spark.sql("select * from Students where marks between 50 and 90");
		gretermarkdf.show();
		
		Dataset<Row> markwithgrades=spark.sql("select S.name, S.marks, G.grade from Students S Inner join Grades G on S.marks between G.startmarks and G.endmarks");
		markwithgrades.show();
		System.out.println("============>>");
		// Adding column to DataFrame //
		Dataset<Row> newDs = studentdf.withColumn("Grade",functions.lit(1));
		newDs.show();
	}

}
