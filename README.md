# Iphone-data


Apple Table CSV File Columns
Product Name: The name of the product listed.
Product URL: The URL or link to the product's webpage or listing.
Brand: The brand or manufacturer of the product.
Sale Price: The price at which the product is currently being sold.
MRP (Maximum Retail Price): The maximum price at which the product can be sold, typically provided by the manufacturer.
Discount Percentage: The percentage of discount applied to the product's sale price compared to its MRP.
Number Of Ratings: The total number of ratings or reviews the product has received.
Number Of Reviews: The total number of written reviews the product has received.
UPC (Universal Product Code): A unique identifier for the product, often used in retail.
Star Rating: The average star rating of the product based on user reviews.
RAM: Random Access Memory (RAM) specification of the product, if applicable.



1. Loaded the CSV file into a Spark DataFrame.
2. Displayed the first 3 rows of the DataFrame and printed its schema.
3. Counted the number of rows in the DataFrame.
4. Selected and displayed the "Sale Price" column for the first 5 rows.
5. Found the maximum sale price in the DataFrame using both DataFrame API and Spark SQL.
6. Found the maximum and minimum MRP (Maximum Retail Price) in the DataFrame.
7. Displayed more rows of the DataFrame, including the first 6 rows.
8. Selected and displayed the "Sale Price" and "MRP" columns for the first row.
9. Filtered the DataFrame to show rows where the MRP is equal to 49900 and 149900 respectively.
10. Filtered the DataFrame to show rows where the MRP is greater than 49900, displaying the first 6 rows.
11. Created a temporary view named "Apple" to use Spark SQL.
12. Ran SQL queries using `%sql` magic to perform aggregation and filtering on the "Apple" table.
13. Created a new column "Discounted_Price" in the DataFrame by applying a discount of 10% on the MRP.
14. Ran a SQL query to sum the MRPs for each product and filtered the results to show only those with a sum greater than 100000.
15. Created a new DataFrame column "new_price" by subtracting the discount from the MRP and displayed the top 10 rows ordered by the new price in descending order.

