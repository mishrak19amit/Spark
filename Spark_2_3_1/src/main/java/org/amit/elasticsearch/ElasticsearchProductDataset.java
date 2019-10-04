package org.amit.elasticsearch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ElasticsearchProductDataset {

	public static void main(String[] args) {
		//13.234.108.103:9201
SparkSession sc = SparkSession.builder().appName("Elastic_search_Product").master("local[*]").getOrCreate();
		
        Dataset<Row> payload = sc
                .read()
                .format( "org.elasticsearch.spark.sql" )
                .option("es.nodes", "http://127.0.0.1:9200/")
                .option("es.read.field.as.array.include", "hashtags")
                //.option("es.port", "9201")
                .option("spark.es.nodes.client.only", true)
                .option("es.http.timeout", "1m")
                .load("twitter/_doc");
        
        payload.printSchema();
        
        payload.createOrReplaceTempView("ES_Product");
        
        sc.sql("select * from ES_Product").show();
		
	}
}
