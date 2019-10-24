package org.amit.orderdataanalysis;

import org.amit.orderanalysis.Util.OrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UserSpecificDataAnalysis {

	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("OrderItemDataPrepration").master("local[*]").getOrCreate();

		Dataset<Row> orderItemJoinedData=OrderUtil.getDatasetFromJSONFile("/home/moglix/orderDataAnalysis/OnlineDatabaseDump/joinedOrderItemDS/joinedOrderItemDS.json", sc);
		userMSNOrderedCount(orderItemJoinedData, sc);
	}

	private static void userMSNOrderedCount(Dataset<Row> orderItemJoinedData, SparkSession sc) {
		
		orderItemJoinedData.createOrReplaceTempView("OrderItems");
		
		Dataset<Row> userIDMSNCount=sc.sql("select user_id, product_id, product_quantity from OrderItems order by product_quantity desc, product_id");
		
		OrderUtil.saveDSAsJson(userIDMSNCount, "userIDMSNOrderCount");
	
		userIDMSNCount.show();
		
	}

}