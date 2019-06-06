package org.amit.Spark_2_3_1;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateDataSet {
	
	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("Craete_Dataset").master("local").getOrCreate();
		 StructField[] structFields = new StructField[]{
		            new StructField("intColumn", DataTypes.IntegerType, true, Metadata.empty()),
		            new StructField("stringColumn", DataTypes.StringType, true, Metadata.empty())
		    };
		 
		 StructField[] demoStructField= new StructField[] {
				 new StructField("name", DataTypes.StringType, true, Metadata.empty()),
				 new StructField("age", DataTypes.IntegerType, true, Metadata.empty())
		 };
		 
		 StructType demoStructType=new StructType(demoStructField);
		 
		 List<Row> demorows=new ArrayList<Row>();
		 
		 demorows.add(RowFactory.create("Amit", 24));
		 demorows.add(RowFactory.create("Ramesht", 25));
		 
		 Dataset<Row> demoDataset= sc.createDataFrame(demorows, demoStructType);
		 
		 demoDataset.printSchema();
		 demoDataset.show();

		    StructType structType = new StructType(structFields);

		    List<Row> rows = new ArrayList<>();
		    rows.add(RowFactory.create(1, "v1"));
		    rows.add(RowFactory.create(2, "v2"));

		    Dataset<Row> df = sc.createDataFrame(rows, structType);
		    df.printSchema();
		    df.createOrReplaceTempView("Dataset");
		    sc.sql("select * from Dataset").show();
		    
	}

}
