package com.learning.sparkdataengg.setup;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.log4j.Level;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

public class SetupPrerequisites {



    private static Logger logger = Logger.getLogger(SetupPrerequisites.class.getName());

    public static void main(String[] args) {

        System.out.println("Starting setup of Spark Data Engineering pre-requisites..");
        try{

            org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN );
            org.apache.log4j.Logger.getLogger("akka").setLevel(Level.WARN);

            System.out.println("----------------- Setting up MariaDB------------");
            createMariaDBSchema();

            System.out.println("----------------- Setting up Kafka---------------- ");
            //Create Kafka Topics
            createKafkaTopic("spark.streaming.website.visits");
            createKafkaTopic("spark.streaming.carts.abandoned");
            createKafkaTopic("spark.exercise.lastaction.long");
        }
        catch(Exception e) {
            e.printStackTrace();
            System.out.println("Unable to setup pre-requisites. Aborting. " +
                    " Please check previous errors");
        }

    }

    private static void createMariaDBSchema() throws Exception {

        System.out.println("Connecting to Maria DB");
        Class.forName("org.mariadb.jdbc.Driver");

        //Connect to MariaDB
        Connection mariaConn = DriverManager.getConnection(
                             "jdbc:mariadb://localhost:3307/mysql",
                            "root",
                         "spark");

        //Check list of databases available
        ResultSet rsDB = mariaConn.getMetaData().getCatalogs();
        List<String> schemaList = new ArrayList<String>();

        while (rsDB.next() ) {
            schemaList.add(rsDB.getString(1));
        }
        System.out.println("Databases available in MariaDB : " + schemaList.toString());

        if ( schemaList.contains( "warehouse_stock")) {
            System.out.println("warehouse_stock DB already available");
        }
        else {
            System.out.println("Going to create warehouse_stock DB");
            Statement schemaStmt = mariaConn.createStatement();
            schemaStmt.executeUpdate("CREATE DATABASE warehouse_stock");
            schemaStmt.executeUpdate("CREATE TABLE `warehouse_stock`.`item_stock` (" +
                                        "  `ID` INT NULL AUTO_INCREMENT," +
                                        "  `STOCK_DATE` DATETIME NOT NULL," +
                                        "  `WAREHOUSE_ID` VARCHAR(45) NOT NULL," +
                                        "  `ITEM_NAME` VARCHAR(45) NOT NULL," +
                                        "  `OPENING_STOCK` INT NOT NULL DEFAULT 0," +
                                        "  `RECEIPTS` INT  NOT NULL DEFAULT 0," +
                                        "  `ISSUES` INT  NOT NULL DEFAULT 0," +
                                        "  `UNIT_VALUE` DECIMAL(10,2)  NOT NULL DEFAULT 0," +
                                        "  PRIMARY KEY (`ID`)," +
                                        "  INDEX `STOCK_DATE` (`STOCK_DATE` ASC));");
            schemaStmt.executeUpdate("GRANT ALL PRIVILEGES ON warehouse_stock.* to 'spark'@'%'");
            schemaStmt.executeUpdate("FLUSH PRIVILEGES");
            schemaStmt.close();
        }

        if ( schemaList.contains( "global_stock")) {
            System.out.println("global_stock DB already available");
        }
        else {
            System.out.println("Going to create global_stock DB");
            Statement schemaStmt = mariaConn.createStatement();
            schemaStmt.executeUpdate("CREATE DATABASE global_stock");
            schemaStmt.executeUpdate("CREATE TABLE `global_stock`.`item_stock` (" +
                    "  `ID` INT NULL AUTO_INCREMENT," +
                    "  `STOCK_DATE` DATETIME NOT NULL," +
                    "  `ITEM_NAME` VARCHAR(45) NOT NULL," +
                    "  `TOTAL_REC` INT NOT NULL DEFAULT 0," +
                    "  `OPENING_STOCK` INT NOT NULL DEFAULT 0," +
                    "  `RECEIPTS` INT  NOT NULL DEFAULT 0," +
                    "  `ISSUES` INT  NOT NULL DEFAULT 0," +
                    "  `CLOSING_STOCK` INT NOT NULL DEFAULT 0," +
                    "  `CLOSING_VALUE` DECIMAL(10,2)  NOT NULL DEFAULT 0," +
                    "  PRIMARY KEY (`ID`)," +
                    "  INDEX `STOCK_DATE` (`STOCK_DATE` ASC));");
            schemaStmt.executeUpdate("GRANT ALL PRIVILEGES ON global_stock.* to 'spark'@'%'");
            schemaStmt.executeUpdate("FLUSH PRIVILEGES");
            schemaStmt.close();
        }

        if ( schemaList.contains( "website_stats")) {
            System.out.println("website_stats DB already available");
        }
        else {
            System.out.println("Going to create website_stats DB");
            Statement schemaStmt = mariaConn.createStatement();
            schemaStmt.executeUpdate("CREATE DATABASE website_stats");
            schemaStmt.executeUpdate("CREATE TABLE `website_stats`.`visit_stats` (" +
                                            "`ID` int(11) NOT NULL AUTO_INCREMENT, " +
                                            "`INTERVAL_TIMESTAMP` DATETIME DEFAULT NULL," +
                                            "`LAST_ACTION` varchar(45) DEFAULT NULL," +
                                            "`DURATION` int(10) DEFAULT NULL, " +
                                            " PRIMARY KEY (`ID`)) " );
            schemaStmt.executeUpdate("GRANT ALL PRIVILEGES ON website_stats.* to 'spark'@'%'");
            schemaStmt.executeUpdate("FLUSH PRIVILEGES");
            schemaStmt.close();
        }

    }

     public static void createKafkaTopic(String topicName) throws Exception {


        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","localhost:9092");

        AdminClient adminClient = AdminClient.create(kafkaProps);

        //Check current list of topics
        Set<String> presentTopics =adminClient.listTopics().names().get();
        System.out.println("Current Topic List :" + presentTopics.toString());

        if (presentTopics.contains(topicName)) {
            System.out.println("Topic already exists : " + topicName);
            return;
        }

        System.out.println("Going to create Kafka topic " + topicName);
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);

        adminClient.createTopics(newTopics);
        adminClient.close();

    }
}
