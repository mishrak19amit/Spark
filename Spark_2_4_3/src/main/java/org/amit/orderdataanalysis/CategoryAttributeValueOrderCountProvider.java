package org.amit.orderdataanalysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.amit.model.CategoryBrandAttributeValueOrderCount;
import org.amit.model.ProductBrandCategoryAttribute;
import org.amit.orderanalysis.Util.OrderUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CategoryAttributeValueOrderCountProvider {

	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("CategoryAttributeValueOrderCountProvider").master("local[*]")
				.getOrCreate();
		Dataset<CategoryBrandAttributeValueOrderCount> prodBrandCatAttributeOrderCount = getDatasetFromFile(sc,
				"/home/moglix/orderDataAnalysis/orderProductDetails/MSNBrandCatAttributeOrderCount/MSNBrandCatAttributeOrderCount.json");

		prodBrandCatAttributeOrderCount.show();
		OrderUtil.saveCategoryBrandAttributeValueOrderCountDS(prodBrandCatAttributeOrderCount, "BrandCatAttributeValOrderCount");
	}

	private static Dataset<CategoryBrandAttributeValueOrderCount> getDatasetFromFile(SparkSession sc, String filePath) {
		Dataset<Row> orderDS = OrderUtil.getDatasetFromJSONFile(filePath, sc);

		Dataset<CategoryBrandAttributeValueOrderCount> CategoryAttributeCountDS = createPojoForJavaObject(orderDS);
		
		System.out.println(CategoryAttributeCountDS.count());
		
		return CategoryAttributeCountDS;

	}

	private static Dataset<CategoryBrandAttributeValueOrderCount> createPojoForJavaObject(Dataset<Row> orderDS) {

		Encoder<CategoryBrandAttributeValueOrderCount> categoryAttributeCountEncoder = Encoders
				.bean(CategoryBrandAttributeValueOrderCount.class);

		Dataset<CategoryBrandAttributeValueOrderCount> categoryAttributeCountRDD = orderDS.toJSON().flatMap(x -> {

			List<CategoryBrandAttributeValueOrderCount> productBrandCategoryAttributeList = new ArrayList<>();
			try {
				ProductBrandCategoryAttribute productBrandCategoryAttribute = new ObjectMapper()
						.readValue(x, ProductBrandCategoryAttribute.class);
				
				Map<String, List<String>> attributeKeyVal=productBrandCategoryAttribute.getAttributeList();
				
				String categoryID=productBrandCategoryAttribute.getCategoryID();
				String brandID=productBrandCategoryAttribute.getBrandID();
				String orderCount=productBrandCategoryAttribute.getOrderCount();
				
				for(String attribute: attributeKeyVal.keySet()) {
					
					CategoryBrandAttributeValueOrderCount categoryBrandAttributeValueOrderCount = new CategoryBrandAttributeValueOrderCount();
					categoryBrandAttributeValueOrderCount.setBrandID(brandID);
					categoryBrandAttributeValueOrderCount.setCategoryID(categoryID);
					categoryBrandAttributeValueOrderCount.setOrderCount(orderCount);
					categoryBrandAttributeValueOrderCount.setAttributeID(attribute);
					categoryBrandAttributeValueOrderCount.setValue(attributeKeyVal.get(attribute).toString().replace("[", "").replace("]", ""));
					productBrandCategoryAttributeList.add(categoryBrandAttributeValueOrderCount);
				}
				
				
			} catch (Exception e) {
				System.out.println(e);
			}

			return productBrandCategoryAttributeList.iterator();
		}, categoryAttributeCountEncoder);


		return categoryAttributeCountRDD;
	}

}
