package com.learning.sparkdataengg.chapter3;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import java.time.*;

/****************************************************************************
 * This Generator generates a series of data files in the raw data folder
 * The files contain simulated data about sales orders
 * This can be used for batch consumption by Apache Spark
 ****************************************************************************/

public class RawStockDataGenerator {

    public static void main(String[] args) {
        generateData("NewYork");
        generateData("LosAngeles");
        generateData("London");
    }

    private static void generateData(String warehouseId) {

        System.out.println("Going to add warehouse records for " + warehouseId);
        try {
            //Master list of items and values
            Map<String,Double> itemValues = new HashMap<String,Double>();
            itemValues.put("Tape Dispenser",5.99);
            itemValues.put("Pencil Sharpener",10.00);
            itemValues.put("Labeling Machine",25.00);
            itemValues.put("Calculator",14.99);
            itemValues.put("Scissors",7.99);
            itemValues.put("Sticky Notes",2.00);
            itemValues.put("Notebook",2.50);
            itemValues.put("Clipboard",12.00);
            itemValues.put("Folders",1.00);
            itemValues.put("Pencil Box",2.99);

            //Define a random number generator
            Random random = new Random();

            //Set local date to 1-June-2021.
            LocalDate startDate = LocalDate.of(2021,6,1);

            //Setup DB connection & Insert Statement
            Class.forName("org.mariadb.jdbc.Driver");
            //Connect to MariaDB
            Connection warehouseConn = DriverManager.getConnection(
                    "jdbc:mariadb://localhost:3307/warehouse_stock",
                    "spark",
                    "spark");

            String insertSql =
                    "INSERT INTO `warehouse_stock`.`item_stock`\n" +
                    "(`STOCK_DATE`,`WAREHOUSE_ID`,`ITEM_NAME`,\n" +
                    "`OPENING_STOCK`,`RECEIPTS`,`ISSUES`,`UNIT_VALUE`)\n" +
                    "VALUES\n" +
                    "(?,?,?,?,?,?,?)";

            PreparedStatement psStock=
                    warehouseConn.prepareStatement(insertSql);

            //Generate simulated stock records

            //Do for 3 days
            for(int i=0;i < 3;i++) {

                Iterator<String> itemIterator = itemValues.keySet().iterator();
                while (itemIterator.hasNext()) {

                    //Set values
                    LocalDate stockDate = startDate.plusDays(i);
                    String item = itemIterator.next();
                    Double unitValue = itemValues.get(item);
                    int openingStock = random.nextInt(100);
                    int receipts = random.nextInt(50);
                    int issues = random.nextInt(openingStock + receipts);

                    psStock.setDate(1, Date.valueOf(stockDate));
                    psStock.setString(2, warehouseId);
                    psStock.setString(3, item);
                    psStock.setInt(4, openingStock);
                    psStock.setInt(5, receipts);
                    psStock.setInt(6, issues);
                    psStock.setDouble(7, unitValue);

                    System.out.println("Adding Record :"
                            + psStock.toString().replace("\n"," "));

                    psStock.executeUpdate();
                }
            }
            warehouseConn.commit();
            warehouseConn.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
