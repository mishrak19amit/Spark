package org.amit.elasticsearch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ElasticsearchProductDataset {

	public static void main(String[] args) {
SparkSession sc = SparkSession.builder().appName("Elastic_search_Product").master("local[*]").getOrCreate();
		
        Dataset<Row> payload = sc
                .read()
                .format( "org.elasticsearch.spark.sql" )
                .option("es.nodes", "http://13.234.108.103:9201/")
                //.option("es.port", "9201")
                .option("spark.es.nodes.client.only", true)
                .option("es.http.timeout", "5m")
                .load("product_v1/_doc");
        
        payload.printSchema();
        
        payload.createOrReplaceTempView("ES_Product");
        
        sc.sql("select * from ES_Product").show();
		
	}
}
