### vinzglobal_project


##Assignment for Databricks
Problem Statement: Design a data model for a scenario where customers place orders. Each order consists of multiple line items representing different products, quantities, and status. Once an order is completely delivered, it's marked as "Completed". Additionally, the status of each line item is tracked.

###Tasks:

Create a model for the above system and define the entity-relationship between the objects.
Provide queries to:
1. Calculate orders by date.
2. Calculate the total order amount by date.
3. Determine the total orders by product per week.
4. Identify the most sold product in the last month.


## Solution Overview
The solution comprises scripts developed and exported from Databricks Community Edition. The scripts for customers, products, and orders read data from a specified path location. Here's an outline of the process:

1. Data Ingestion: The scripts read CSV or JSON files and create Delta tables.
2. Delta Table Creation: After creating the Delta table, a final table is generated to store data prepared for consumption.
3. Final Table Operation: The final table operates in UPSERT mode, ensuring historical data integrity.
4. Analysis Files: The analysis files execute four queries in SQL and Python to answer the data analysis questions posed earlier.
5. This structured approach ensures efficient data management and analysis capabilities, facilitating insights extraction and decision-making processes.
