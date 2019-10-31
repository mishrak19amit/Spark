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

		//findMostSoledCategory(orderDetails, sc);
		//findMostSoledCategoryAttribute(orderDetails, sc);
		//findMostSoledAttributeOfCategory(orderDetails,sc);
		findMostSoledCategoryOfBrand(orderDetails,sc);
	}

	private static void findMostSoledCategoryAttribute(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql(
				"select category_name, attribute_name, sum(orderCount) as totalOrders from orderDetails where group by category_name, attribute_name order by totalOrders desc, category_name");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "mostSoledCategoryAttribute");
		dataset.show();
	}

	private static void findMostSoledCategory(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql("select category_name, sum(orderCount) as totalOrders from orderDetails group by category_name order by totalOrders desc");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "mostSoledCategory");
		dataset.show();
		
	}
	
	private static void findMostSoledAttributeOfCategory(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql("select category_name, attribute_name, value, sum(orderCount) as totalOrders from orderDetails where group by category_name, attribute_name, value order by totalOrders desc");
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "mostSoledAttributeOfCategory");
		dataset.show();
		
	}
	
	private static void findMostSoledCategoryOfBrand(Dataset<Row> orderDetails, SparkSession sc) {
		orderDetails.createOrReplaceTempView("orderDetails");

		Dataset<Row> dataset = sc.sql("select brand_name, category_name, sum(orderCount) as totalOrders from orderDetails where group by brand_name, category_name order by brand_name, totalOrders desc");
		//SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(dataset, "findMostSoledCategoryOfBrand");
		dataset.show();
		
	}

}
