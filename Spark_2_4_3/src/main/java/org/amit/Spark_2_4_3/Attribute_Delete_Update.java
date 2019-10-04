package org.amit.Spark_2_4_3;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Attribute_Delete_Update {

	public static void main(String[] args) {
//		SparkSession sc = SparkSession.builder().appName("Dataset_CSV").master("local").getOrCreate();
//		Dataset<Row> csvdf = sc.read().format("csv").option("header", true)
//				.load("/home/moglix/Downloads/Item In PAck-In-Active Product.csv");
//
//		csvdf.printSchema();
//
//		csvdf.createOrReplaceTempView("INActive_Product");
//
//		sc.sql("select * from INActive_Product ").show();
		
		SparkConf conf= new SparkConf().setAppName("Dataset_CSV").setMaster("local[*]");
		JavaSparkContext sc= new JavaSparkContext(conf);
//		JavaRDD<String> fileVal=sc.textFile("/home/moglix/Downloads/Item In PAck-In-Active Product.csv").flatMap(f->{
//			String vals[]=f.split(",");
//			
//			List<String> valList=new ArrayList<>();
//			valList.add("UPDATE products.product_data SET product_type='"+vals[1]+"', uom='"+vals[2]+"', quantity_uom="+vals[3]+", Items_in_Pack='"+vals[4]+"' WHERE id_product='"+vals[0]+"';");
//			valList.add("UPDATE products.product_data_raw SET attribute_list = attribute_list - {bb85c6a9-c33b-4e1c-a108-60d340e765d4,9463b45a-7d84-46ae-9847-a37fb3d4098b,22e04e62-31ab-4a77-a5d1-ee0234f9e830} WHERE id_product='"+vals[0]+"';");
//			return valList.iterator();
//		});
		
		JavaRDD<String> msnList=sc.textFile("/home/moglix/Downloads/Item In PAck-Active Product.csv").map(x->{
			String vals[]=x.split(",");
			return vals[0];
		});
		
		String msnListComman="";
		for(String s: msnList.collect()) {
			
			msnListComman+=s+",";
		}
		
		System.out.println(msnListComman);
		
//		fileVal.foreach(x->System.out.println(x));
		
//		for(String s: fileVal.take(20)) {
//			System.out.println(s);
//		}
//		
//		fileVal.repartition(1).saveAsTextFile("/home/moglix/Desktop/Task/Active_Product_AttributeDelete");
//		
		sc.close();

		//sc.sql("SELECT State, SUM(`Aadhaar generated`) as count FROM Aadhar GROUP BY state ORDER BY count DESC").show();
	}

}
