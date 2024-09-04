package com.learning.sparkdataengg.chapter3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class GlobalStockDBBrowser {
    public static void main(String[] args) {

        try{
            System.out.println("Records Summary in Global Stock DB: ");

            Connection mariaConn =
                    DriverManager.getConnection(
                    "jdbc:mariadb://localhost:3307/global_stock",
                    "spark",
                    "spark");

            String globalStockQuery =
                    "SELECT STOCK_DATE, count(*) " +
                            "FROM global_stock.item_stock " +
                            "GROUP BY STOCK_DATE";

            ResultSet rsStock =
                    mariaConn
                            .createStatement()
                            .executeQuery(globalStockQuery);

            while (rsStock.next()) {
                System.out.println("Date :" + rsStock.getString(1) +
                                    ", Records : " + rsStock.getInt(2)) ;
            }

            mariaConn.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
