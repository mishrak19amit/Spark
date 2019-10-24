package org.amit.apiloganalysis;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SearchApiResponseTime {

	private static JavaSparkContext jsc;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Search Response Time").setMaster("local[*]");
		jsc = new JavaSparkContext(conf);

		//saveSearchKeywordAndResponseTime();

		//getTotalTimeSpendOnSearch();

		getFileNameForSearckKeyword("age minimize 3d 4-in-1 cleanser",
				"/home/moglix/Desktop/Task/SearchResponseEnhance/searchapilogs/backend2-logs/api");
		
		getFileNameForSearckKeyword("queen greatest hits parts 1 & 2",
				"/home/moglix/Desktop/Task/SearchResponseEnhance/searchapilogs/backend2-logs/api");

		jsc.close();
	}

	private static void getFileNameForSearckKeyword(String searchKeyword, String folderPath) {

		JavaRDD<String> listOfFilesRDD = jsc.parallelize(getListOfFile(folderPath));

		listOfFilesRDD.take(10).forEach(x -> System.out.println(x));

		JavaPairRDD<JavaRDD<String>, String> fileRDD = getPairedJavaRDDAndFileNmae(listOfFilesRDD);
		
		JavaPairRDD<String, String> fileNameSearckKeyword = fileRDD.mapToPair(x -> {

			JavaRDD<String> logRDD = x._1();
			if (logRDD.filter(val -> val.contains(searchKeyword)).count() > 0) {
				return new Tuple2<String, String>(searchKeyword, x._2());
			}

			else {
				return null;
			}

		});

		//fileNameSearckKeyword.filter(x->x!=null).collect().forEach(x -> System.out.println(x));
		List<Tuple2<String, String>> fileKeyword=fileNameSearckKeyword.take((int)listOfFilesRDD.count());
		//System.out.println(fileNameSearckKeyword.count());
		// System.out.println(fileRDD.count());

		for(Tuple2<String, String> val: fileKeyword) {
			if(null!=val) {
				System.out.println(val);
			}
		}
		
		System.out.println("");
	}
	
	private static JavaPairRDD<JavaRDD<String>, String> getPairedJavaRDDAndFileNmae(JavaRDD<String> listOfFilesRDD){
		JavaPairRDD<JavaRDD<String>, String> fileRDD = listOfFilesRDD.mapToPair(x -> {
			return new Tuple2<JavaRDD<String>, String>(getFileLogRDD(x), x);

		});
		
		fileRDD.cache();
		
		return fileRDD;
		
	}

	private static JavaRDD<String> getFileLogRDD(String fileName) {

		return jsc.textFile(fileName);

	}

	private static List<String> getListOfFile(String folderPath) {
		File folder = new File(folderPath);
		File[] listOfFiles = folder.listFiles();

		List<String> listOfFilesToreturn = new ArrayList<String>();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				if (listOfFiles[i].toString().contains("searchAPI-")
						|| listOfFiles[i].toString().contains("searchAPI.log")) {
					listOfFilesToreturn.add(listOfFiles[i].toString());
				}
			}
		}

		System.out.println(listOfFilesToreturn.size());
		
		return listOfFilesToreturn;
	}

	private static void getTotalTimeSpendOnSearch() {

		JavaRDD<String> logLine = getFileLogRDD(
				"/home/moglix/Desktop/Task/SearchResponseEnhance/backen2SearchResponseTime");

		JavaRDD<Integer> timeRDD = logLine.map(x -> {
			String[] sepratedForTime = x.split(" ");

			if (2 <= sepratedForTime.length) {
				int searchTime = 0;
				try {

					searchTime = Integer.parseInt(sepratedForTime[0]);
				} catch (Exception e) {
					// System.out.println("Number format exception" + e);
				}

				return searchTime;
			} else {
				return 0;
			}
		});

		int totalTime = timeRDD.reduce((a, b) -> a + b);

		long totalSearch = timeRDD.count();

		long avgTime = totalTime / totalSearch;

		System.out.println("Total time spent for searching: " + totalTime + " | Total search happened: " + totalSearch
				+ " | Aveg time: " + avgTime);
	}

	public static void saveSearchKeywordAndResponseTime() {

		JavaRDD<String> logLine = getFileLogRDD(
				"/home/moglix/Desktop/Task/SearchResponseEnhance/backen2SearchResponseTime");

		JavaPairRDD<Integer, String> searchKeyTime = logLine.mapToPair(x -> {

			String[] sepratedForTime = x.split(" ");
			String[] sepratedForKeyword = x.split("searchString=");
			if (2 <= sepratedForTime.length && 2 <= sepratedForKeyword.length) {
				int searchTime = 0;
				try {

					searchTime = Integer.parseInt(sepratedForTime[0]);
				} catch (Exception e) {
					// System.out.println("Number format exception" + e);
				}

				return new Tuple2<Integer, String>(searchTime, sepratedForKeyword[1]);
			} else {
				return new Tuple2<Integer, String>(1, "");
			}

		});

		JavaPairRDD<Integer, String> searchKeyTimeSorted = searchKeyTime.sortByKey(false);

		searchKeyTimeSorted.take(10)
				.forEach(x -> System.out.println("search Time: " + x._1() + " | searck Keyword : " + x._2()));

		searchKeyTimeSorted.repartition(1)
				.saveAsTextFile("/home/moglix/Desktop/Task/SearchResponseEnhance/searchTimeWithKeyword");

	}
}
