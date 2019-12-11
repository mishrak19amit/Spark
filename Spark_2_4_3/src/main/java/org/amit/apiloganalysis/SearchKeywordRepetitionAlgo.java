package org.amit.apiloganalysis;

import org.amit.mysql.read.write.dataset.SaveDatasetToMysqlTableUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SearchKeywordRepetitionAlgo {

	public static void main(String[] args) {
		SparkSession sc = SparkSession.builder().appName("searckKeywordRepetition").master("local[*]").getOrCreate();
		Dataset<Row> previousSearchKeyWord = getSearchDatasetFromCSVFile(sc,
				"/home/moglix/searchKeywordAnalysis/searchKeywordOnly.csv");
		Dataset<Row> searchKeyWord = getSearchDatasetFromCSVFile(sc,
				"/home/moglix/searchKeywordAnalysis/8nov5decSearchLog/searchStringBackend.csv");

		Dataset<Row> newlySearchKeyWord = getNewlySearchDataset(sc, previousSearchKeyWord, searchKeyWord);

		//System.out.println("search count: " + searchKeyWord.count() + " matched search count: " + newlySearchKeyWord.count());
		//newlySearchKeyWord.show();
		SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(newlySearchKeyWord,"newlySearchKeyWordRepetition");

	}

	private static Dataset<Row> getSearchDatasetFromCSVFile(SparkSession sc, String filePath) {
		/// home/moglix/searchKeywordAnalysis/searchKeywordOnly.csv

		Dataset<Row> searchKeyWord = sc.read().format("csv").option("header", false).load(filePath);
		searchKeyWord.createOrReplaceTempView("searckKeywordOld");

		Dataset<Row> searchKeyWordLower = sc.sql(
				"select TRIM(LOWER(_c0)) as searchKeyword , count(_c0) as repetition from searckKeywordOld group by searchKeyword order by repetition desc");
		searchKeyWordLower.show();
		// SaveDatasetToMysqlTableUtil.saveDatasetToMysqlTable(searchKeyWordLower,
		// "searchKeywordRepetition");
		// searchKeyWordLower.repartition(1).write().format("csv").save("/home/moglix/searchKeywordAnalysis/searckKeywordRepetitionCount");
		// System.out.println(searchKeyWordLower.count());
		return searchKeyWordLower;

	}

	private static Dataset<Row> getNewlySearchDataset(SparkSession sc, Dataset<Row> previousSearchKeyWord,
			Dataset<Row> newSearchKeyWord) {

		previousSearchKeyWord.show();
		newSearchKeyWord.show();

		previousSearchKeyWord.createOrReplaceTempView("previousSearchKeyWord");
		newSearchKeyWord.createOrReplaceTempView("newSearchKeyWord");
		Dataset<Row> duplicateSearchString = sc.sql(
				"select newSearchKeyWord.searchKeyword, newSearchKeyWord.repetition from previousSearchKeyWord join newSearchKeyWord on previousSearchKeyWord.searchKeyword=newSearchKeyWord.searchKeyword");
		
		Dataset<Row> newlySearchString = newSearchKeyWord.exceptAll(duplicateSearchString);
		
		System.out.println(newSearchKeyWord.count()+" "+newlySearchString.count());
		//System.out.println(searchString.count());
		
		return newlySearchString;

	}


}
