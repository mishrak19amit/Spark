package org.amit.Spark_2_3_1;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateDataset3 {

	public static void main(String[] args) {

		SparkSession sc = SparkSession.builder().appName("using_Text_Dataset").master("local").getOrCreate();
		JavaRDD<String> peopleRDD = sc.sparkContext()
				.textFile("/home/moglix/Desktop/Amit/Spark2.3.1/spark/examples/src/main/resources/people.txt", 1)
				.toJavaRDD();
		String schemaString = "name age";

		List<StructField> fields = new ArrayList<>();

		for (String strfield : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(strfield, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = peopleRDD.map(record -> {
			String[] attributes = record.split(",");
			return RowFactory.create(attributes[0], attributes[1].trim());
		});
		Dataset<Row> peopleDataFrame = sc.createDataFrame(rowRDD, schema);
		peopleDataFrame.select(col("name")).show();
	}
}
