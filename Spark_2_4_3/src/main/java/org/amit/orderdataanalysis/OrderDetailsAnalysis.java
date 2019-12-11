package org.amit.orderdataanalysis;


import org.amit.mysql.read.write.dataset.SaveDatasetToMysqlTableUtil;
import org.amit.orderanalysis.Util.OrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OrderDetailsAnalysis {

	public static void main(String[] args) {

		SparkSession sc = SparkSession.builder().appName("OrderDetailsAnalysis").master("local[*]").getOrCreate();

		Dataset<Row> orderDetails = OrderUtil.getDatasetFromJSONFile(
				"/home/moglix/orderDataAnalysis/OnlineDatabaseDump/BrandCatAttributeValOrderCount/BrandCatAttributeValOrderCount.json",
				sc);

		orderDetails.printSchema();

		//findMostSoldCategory(orderDetails, sc);
		//findMostSoldCategoryAttribute(orderDetails, sc);
		//findMostSoldAttributeOfCategory(orderDetails,sc);
		findMostSoldCategoryOfBrand(orderDetails,sc);
	}

	private static void findMostSoldCategoryAttribute(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql(
				"select category_name, attribute_name, sum(orderCount) as totalOrders from orderDetails where group by category_name, attribute_name order by totalOrders desc, category_name");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "mostSoldCategoryAttribute");
		dataset.show();
	}

	private static void findMostSoldCategory(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql("select category_name, sum(orderCount) as totalOrders from orderDetails group by category_name order by totalOrders desc");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "mostSoldCategory");
		dataset.show();
		
	}
	
	private static void findMostSoldAttributeOfCategory(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql("select category_name, attribute_name, value, sum(orderCount) as totalOrders from orderDetails where group by category_name, attribute_name, value order by totalOrders desc");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "mostSoldAttributeOfCategory");
		dataset.show();
		
	}
	
	private static void findMostSoldCategoryOfBrand(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql("select brand_name, category_name, sum(orderCount) as totalOrders from orderDetails where group by brand_name, category_name order by brand_name, totalOrders desc");
		//SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "findMostSoldCategoryOfBrand");
		dataset.show();
		
	}

}
