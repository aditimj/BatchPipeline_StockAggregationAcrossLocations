# BatchPipeline_StockAggregationAcrossLocations

Building a batch processing pipeline that will provide stock aggregation across multiple locations for an enterprise.

# Project Scenario

1. The enterprise consists of multiple warehouses across the globe.
2. These warehouses maintain stock of items from various locations and then distribute them to local stores.
3. Each warehouse has a local data center with required hardware and software deployed in that center.
4. Stock Management application runs in each warehouse. The warehouses consists of same S/W but has local independent instances deployed in local data centers.
5. Here local MariaDB database keeps track of the warehouse stocks.

# Objective:

1. Create and manage a central, consolidated stock database.
2. Setup batch processing to upload warehouse data into central cloud and manage stock.
3. Develop a scalable solution to track data for 100s of warehouses.

This project focused on data gathering, consolidation and aggregation.
