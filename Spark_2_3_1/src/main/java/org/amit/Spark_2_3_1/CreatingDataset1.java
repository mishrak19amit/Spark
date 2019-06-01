package org.amit.Spark_2_3_1;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class CreatingDataset1 {

	public static void main(String[] args) {
		
		SparkSession sc= new SparkSession.Builder().appName("Basic_Spark_Dataset_Example").master("local").getOrCreate();
		List<Person> personList= new ArrayList<Person>();
		for (int i = 0; i < 5; i++) {
			Person person = new Person();
			person.setName("Amit"+i);
			person.setAge(25+i);
			personList.add(person);
		}
		Encoder<Person> personencoder= Encoders.bean(Person.class);
			Dataset<Person> javabeansdf = sc.createDataset(Collections.synchronizedList(personList), personencoder);
			
			javabeansdf.show();
			
			javabeansdf.select(col("name")).show();
			
	Encoder<Integer> integerencoder= Encoders.INT();
	
	Dataset<Integer> integerdataset= sc.createDataset(Arrays.asList(1,2,3,4),integerencoder );
	integerdataset.map(x-> x+1, integerencoder).show();
	
	Dataset<Person> dfperson= sc.read().json("/home/moglix/Desktop/Amit/Spark2.3.1/spark/examples/src/main/resources/people.json").as(personencoder);
	dfperson.show();
	
	}
}
