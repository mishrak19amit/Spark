package org.amit.orderdataanalysis;

import org.amit.orderanalysis.Util.OrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OrderDetailsItemAnalysis {

	public static void main(String[] args) {

		SparkSession sc = SparkSession.builder().appName("OrderItemDataPrepration").master("local[*]").getOrCreate();

		saveJoinedDSOrderItems(sc);
	}

	private static void saveJoinedDSOrderItems(SparkSession sc) {

		String OrderDetalsFileName = "/home/moglix/orderDataAnalysis/OnlineDatabaseDump/order_details/order_details.json";
		String ItemDetalsFileName = "/home/moglix/orderDataAnalysis/OnlineDatabaseDump/order_items/order_items.json";

		Dataset<Row> orderDS = OrderUtil.getDatasetFromJSONFile(OrderDetalsFileName, sc);
		Dataset<Row> itemDS = OrderUtil.getDatasetFromJSONFile(ItemDetalsFileName, sc);

		orderDS.createOrReplaceTempView("OrderDetails");
		itemDS.createOrReplaceTempView("ItemDetails");

		Dataset<Row> orderItemDS = sc.sql(
				"select user_id,product_id,product_quantity,order_id,product_name,cart_id,total_amount,total_amount_with_offer,total_amount_with_taxes,total_offer,OrderDetails.total_payable_amount, OrderDetails.created_at, OrderDetails.updated_at from OrderDetails join ItemDetails on ItemDetails.order_id=OrderDetails.id where product_id !='' order by user_id");

		orderItemDS.show();
		OrderUtil.saveDSAsJson(orderItemDS, "joinedOrderItemDS");

	}

}
