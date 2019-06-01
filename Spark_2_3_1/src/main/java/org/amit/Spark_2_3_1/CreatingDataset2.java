package org.amit.Spark_2_3_1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class CreatingDataset2 {

	public static void main(String[] args) {
		
		SparkSession sc= SparkSession.builder().appName("using_Text_Dataset").master("local").getOrCreate();
		JavaRDD<Person> peopleRDD=sc.read().textFile("/home/moglix/Desktop/Amit/Spark2.3.1/spark/examples/src/main/resources/people.txt")
				.javaRDD()
				.map(line->{ String[] values=line.split(",");
				Person person= new Person();
				person.setName(values[0]);
			    person.setAge(Integer.parseInt(values[1].trim()));
			    return person;
					
				});
		
		Dataset<Row> peopledf= sc.createDataFrame(peopleRDD, Person.class);
		peopledf.select(col("name")).show();
	}
}
