package com.learning.sparkdataengg.chapter3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class StockAggregatorJob {

    public static void main(String[] args) {

        System.out.println("Starting daily stock aggregator Job");

        //Needed for windows only. Use hadoop 3
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        //Set logger levels so we are not going to get info messages from underlying libraries
        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //aggregate stock across warehouses
        aggregateStock("raw_data/");
    }

    private static void aggregateStock(String sourceDir) {
        try {
            //Create the Spcark Session
            SparkSession spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.host","127.0.0.1")
                    .config("spark.driver.bindAddress","127.0.0.1")
                    .config("spark.sql.shuffle.partitions", 2)
                    .config("spark.default.parallelism", 2)
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
                    .appName("StockDataAggregatorJob")
                    .getOrCreate();

            spark.sparkContext().setLogLevel("ERROR");

            Dataset<Row> stockDF
                    = spark.read()
                    .parquet(sourceDir);

            stockDF.printSchema();

            //Create View
           stockDF.createOrReplaceTempView("GLOBAL_STOCK");

           System.out.println("Total Records available : " );
           spark.sql("SELECT count(*) FROM GLOBAL_STOCK").show();

           Dataset<Row> stockSummary
                   = spark.sql(
                           "SELECT STOCK_DATE, ITEM_NAME, " +
                                   "COUNT(*) as TOTAL_REC," +
                                   "SUM(OPENING_STOCK) as OPENING_STOCK, " +
                                   "SUM(RECEIPTS) as RECEIPTS, " +
                                   "SUM(ISSUES) as ISSUES, " +
                                   "SUM( OPENING_STOCK + RECEIPTS - ISSUES) as CLOSING_STOCK, "+
                                   "SUM( (OPENING_STOCK + RECEIPTS - ISSUES) * UNIT_VALUE ) as CLOSING_VALUE " +
                                   "FROM GLOBAL_STOCK " +
                                   "GROUP BY STOCK_DATE, ITEM_NAME"
                          );

           System.out.println("Global Stock Summary: ");
           stockSummary.show();

           //Append to the MariaDB table. Will add duplicate rows if run again
           stockSummary
                   .write()
                   .mode(SaveMode.Append)
                   .format("jdbc")
                   //Using mysql since there is a bug in mariadb connector
                   //https://issues.apache.org/jira/browse/SPARK-25013
                   .option("url", "jdbc:mysql://localhost:3307/global_stock")
                   .option("dbtable", "global_stock.item_stock")
                   .option("user", "spark")
                   .option("password", "spark")
                   .save();

           spark.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
