package org.amit.productAnalysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProductBrandCategoryMapping {
	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("Product_Brand_Category_Mapping").master("local[*]")
				.getOrCreate();
		Dataset<Row> prodBrandCatAttributeOrderCount = getDatasetFromFile(sc,
				"/home/moglix/orderDataAnalysis/ProdCassandraDataset/ProdCassProduct/Product_23oct.json");

		saveDataset(prodBrandCatAttributeOrderCount, "MSNBrandCatAttributeOrderCount");

	}

	private static Dataset<Row> getDatasetFromFile(SparkSession sc, String filePath) {

		Dataset<Row> productDS = getDataset(sc, filePath);
		productDS.printSchema();

		productDS.createOrReplaceTempView("ProductView");

		Dataset<Row> productOrderedCountDS = getProductOrderedCountDataset(sc,
				"/home/moglix/orderDataAnalysis/OnlineDatabaseDump/joinedOrderItemDS/joinedOrderItemDS.json");

		productOrderedCountDS.createOrReplaceTempView("OrderItemCount");

		Dataset<Row> productDSMap = sc.sql(
				"select ProductView.id_product as msn, brand_id as brandID, category_code[0] as categoryID, attribute_list as attributeList, OrderCount as orderCount from ProductView join OrderItemCount on OrderItemCount.MSN=ProductView.id_product  where active=true");

		productDSMap.printSchema();
		productDSMap.show();
		return productDSMap;

	}

	private static Dataset<Row> getProductOrderedCountDataset(SparkSession sc, String filePath) {
		Dataset<Row> orderItemDS = getDataset(sc, filePath);
		orderItemDS.printSchema();
		orderItemDS.createOrReplaceTempView("OrderItem");
		Dataset<Row> productOrderCount = sc.sql(
				"select product_id as MSN, sum(product_quantity) as OrderCount from OrderItem group by product_id");

		return productOrderCount;
	}

	private static Dataset<Row> getDataset(SparkSession sc, String filePath) {
		return sc.read().json(filePath);
	}

	private static void saveDataset(Dataset<Row> dataset, String fileName) {
		dataset.repartition(1).write().format("JSON")
				.save("/home/moglix/orderDataAnalysis/orderProductDetails/" + fileName);
	}
}
