package org.amit.pricingdatamigration;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class MigrationPrice {

public static void main(String[] args) {
SparkConf conf = new SparkConf();
conf.set("spark.driver.memory", "4g");
SparkSession cassandraSc = SparkSession.builder().config(conf).appName("Spark Query Executor")
.master("local[*]").getOrCreate();

// Dataset<Row> productTempDF = cassandraSc.read().format("org.apache.spark.sql.cassandra")
// .option("spark.cassandra.connection.host", "13.234.108.103")
// .option("spark.cassandra.auth.username", "bagira")
// .option("spark.cassandra.auth.password", "Welc0M@2dju9gl@").option("table", "product_pricing")
// .option("keyspace", "products").load();

Dataset<Row> productTempDF = cassandraSc.read().format("org.apache.spark.sql.cassandra")
.option("spark.cassandra.connection.host", "54.169.1.174")
.option("spark.cassandra.auth.username", "bagira")
.option("spark.cassandra.auth.password", "Welc0M@2dju9gl@").option("table", "product_pricing")
.option("keyspace", "products").load();

productTempDF.printSchema();
productTempDF.cache();
productTempDF.createOrReplaceTempView("ProdProduct");

Dataset<Row> updatedDF = cassandraSc.sql(
"SELECT id_product, moglix_supplier_id, offered_price_without_tax as retail_tp, moq as retail_moq, quantity_available, estimated_delivery, out_of_stock_flag, deleted_flag, created_on FROM ProdProduct");

Encoder<ProductPricing> productPricingEncoder = Encoders.bean(ProductPricing.class);
Dataset<ProductPricing> productPricingDF = updatedDF.as(productPricingEncoder);
productPricingDF.show();
// System.out.println("Size : --->> "+productTempDF.count());

Properties prop = new Properties();
prop.setProperty("driver", "com.mysql.cj.jdbc.Driver");
prop.setProperty("user", "root");
prop.setProperty("password", "moglix");

// jdbc mysql url - destination database is named "data"
String url = "jdbc:mysql://localhost:3306/supplier";

// Size : --->> 1110665

//prop.setProperty("user", "bagira");
//prop.setProperty("password", "moglix@rocks1");

//jdbc mysql url - destination database is named "data"
//String url = "jdbc:mysql://13.234.108.103:3307/supplier";


// destination database table
String table = "cass_supplier_products_tmp";

productPricingDF.write().mode(SaveMode.Append).format("jdbc").jdbc(url, table, prop);
System.out.println("--------------END ----------------");
// write data from spark dataframe to database
// productPricingDF.write.mode("append").jdbc(url, table, prop)
// List<ProductPricing> list =productPricingDF.collectAsList();
cassandraSc.close();

}
}
