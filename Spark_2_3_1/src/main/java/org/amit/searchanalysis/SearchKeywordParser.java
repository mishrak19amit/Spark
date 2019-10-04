package org.amit.searchanalysis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SearchKeywordParser {

	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("Search_Keyword_Analysis").master("local[*]").getOrCreate();
		SearchKeywordParser obj= new SearchKeywordParser();
		List<String> files=obj.getFileListFromDirect();
		for(String file: files) {
			try {
				Dataset<Row> keywordDF=obj.getDatasetForCSV(sc, file);
				
				obj.saveToMysql(sc, keywordDF);
			}
			catch(Exception e) {
				System.out.println("Exception occur in file: "+  file);
				System.out.println(e);
			}
			
		}
		
		
		//globalKeywordDF.repartition(1).write().format("CSV").save("/home/moglix/Desktop/Amit/Data_Spark/searchhKeyWordDump");
	}
	
	public Dataset<Row> getDatasetForCSV(SparkSession sc, String file)
	{
		Dataset<Row> searcKeywordDF =sc.read().format("com.crealytics.spark.excel")
			    .option("useHeader", "false")
			    .option("treatEmptyValuesAsNulls", "true")
			    .option("inferSchema", "true")
			    .option("addColorColumns", "False")
			    .option("sheetName", "Dataset1")
			    .load(file);
		
		System.out.println(searcKeywordDF.count());
		
		return searcKeywordDF;
	}
	
	public List<String> getFileListFromDirect(){
		List<String> result= new ArrayList<String>();
		try (Stream<Path> walk = Files.walk(Paths.get("/home/moglix/Downloads/Moglix_Search_Terms_Reports/"))) {

			result = walk.filter(Files::isRegularFile)
					.map(x -> x.toString()).collect(Collectors.toList());

			result.forEach(System.out::println);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	
	public void saveToMysql(SparkSession sc, Dataset<Row> payload)
	{
        payload.createOrReplaceTempView("SearchKeyWord");
        
        String query="select _c0 as keyword, _c1 as total_Unique_Searches, _c2 as results_Pageviews_Search, _c3 as search_Exits, _c4 as search_Refinements, _c5 as time_After_Search, _c6 as avg_Search_Depth from SearchKeyWord";
        
        System.out.println(query);
        
        Dataset<Row> updatedPayload=sc.sql(query);
        
        updatedPayload.printSchema();
        
        //updatedPayload.foreach(x->System.out.println(x));
        
        Dataset<Row> filteredUpdatedPayload=updatedPayload.filter(updatedPayload.col("keyword").notEqual("Search Term"));
        
        //filteredUpdatedPayload.foreach(x->System.out.println(x));
        
        Properties prop = new Properties();
        		prop.setProperty("driver", "com.mysql.jdbc.Driver");
        		prop.setProperty("user", "root");
        		prop.setProperty("password", "moglix"); 
        
        		String url = "jdbc:mysql://localhost:3306/searchAnalysis?useUnicode=true&characterEncoding=utf8";
        				 
        				//destination database table 
        				String table = "searchKeyword";
        				
        				filteredUpdatedPayload.write().mode(SaveMode.Append).jdbc(url, table, prop);
        		
	}
}
