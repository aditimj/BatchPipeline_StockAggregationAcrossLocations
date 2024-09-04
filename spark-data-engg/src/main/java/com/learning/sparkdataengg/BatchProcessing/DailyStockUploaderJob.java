package com.learning.sparkdataengg.chapter3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class DailyStockUploaderJob {

    public static void main(String[] args) {

        System.out.println("Starting daily stock uploader Job");

        //Needed for windows only. Use hadoop 3
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        //Set logger levels so we are not going to get info messages from underlying libraries
        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //Upload for each warehouse
        uploadStock("2021-06-01", "2021-06-03","London");
        uploadStock("2021-06-01", "2021-06-03","LosAngeles");
        uploadStock("2021-06-01", "2021-06-03","NewYork");
    }

    private static void uploadStock(String startDate, String endDate, String warehouseId) {

        try {
            //Make sure the delete the data directory before the run. Sometimes,
            //there will be errors that the process cannot delete the data or temp directories

            System.out.println("Running stock upload for " + warehouseId +
                    " for period " + startDate + " to " + endDate);

            //Run a query to find the min and max IDs for the data to process
            //This is run in the driver program.
            String boundsQuery=
                    "SELECT min(ID) as MIN_ID ,max(ID) as MAX_ID " +
                            "FROM item_stock " +
                            "WHERE STOCK_DATE BETWEEN " +
                            "'" +  startDate + "' AND '" + endDate + "' " +
                            "AND WAREHOUSE_ID = '" + warehouseId + "'";

            //Setup DB connection
            Class.forName("org.mariadb.jdbc.Driver");
            //Connect to MariaDB
            Connection warehouseConn = DriverManager.getConnection(
                    "jdbc:mariadb://localhost:3307/warehouse_stock",
                    "spark",
                    "spark");

            ResultSet rsBounds =
                            warehouseConn
                                .createStatement()
                                .executeQuery(boundsQuery);
            int minBounds=0;
            int maxBounds=0;
            while(rsBounds.next()) {
                minBounds=rsBounds.getInt(1);
                maxBounds=rsBounds.getInt(2);
            }

            System.out.println("Bounds for the Query are "
                    + minBounds + " and " + maxBounds);

            //Create the Spark Session
            SparkSession spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.host","127.0.0.1")
                    .config("spark.driver.bindAddress","127.0.0.1")
                    .config("spark.sql.shuffle.partitions", 2)
                    .config("spark.default.parallelism", 2)
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
                    .appName("StockDailyUploaderJob")
                    .getOrCreate();

            spark.sparkContext().setLogLevel("ERROR");

            String stockQuery=
                    "SELECT ID, date_format(STOCK_DATE,'%Y-%m-%d') as STOCK_DATE," +
                    "WAREHOUSE_ID, ITEM_NAME, OPENING_STOCK, RECEIPTS, ISSUES, UNIT_VALUE " +
                    "FROM item_stock " +
                    "WHERE STOCK_DATE BETWEEN " +
                    "'" +  startDate + "' AND '" + endDate + "' " +
                            "AND WAREHOUSE_ID = '" + warehouseId + "'";

            Dataset<Row> stockDF
                    = spark.read()
                    .format("jdbc")
                    //Using mysql since there is a bug in mariadb connector
                    //https://issues.apache.org/jira/browse/SPARK-25013
                    .option("url", "jdbc:mysql://localhost:3307/warehouse_stock")
                    .option("dbtable", "( " + stockQuery + " ) as tmpStock")
                    .option("user", "spark")
                    .option("password", "spark")
                    .option("partitionColumn","ID")
                    .option("lowerBound", minBounds)
                    .option("upperBound",maxBounds + 1)
                    .option("numPartitions",2)
                    .load();

            //Use same logic to write to HDFS or S3, remember to include
            //required libraries and parameters
            //Spark works much better with shared filesystems
            stockDF.write()
                    .mode(SaveMode.Append)
                    .partitionBy("STOCK_DATE","WAREHOUSE_ID")
                    .parquet("raw_data/");

            System.out.println("Total size of Stock DF : " + stockDF.count());
            stockDF.show();

            spark.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
