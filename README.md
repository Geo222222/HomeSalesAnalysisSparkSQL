# **Home Sales Analysis with SparkSQL**

## **Overview**

This repository contains the solution for the Module 22 Challenge, where we analyze home sales data using SparkSQL. The primary objective is to calculate key metrics related to home sales, utilizing various Spark features such as temporary views, data partitioning, and caching.

## **Project Structure**

- **Home_Sales.ipynb**: Jupyter notebook containing the implementation of the analysis.
- **home_sales_revised.csv**: Dataset used for analysis (downloaded separately).

## **Getting Started**

### **Prerequisites**

Ensure you have the following installed:
- **Apache Spark** with **PySpark**
- **Jupyter Notebook**
- **Python** (version 3.6 or higher)

### **Instructions**

1. **Clone the Repository**
   ```bash
   git clone https://github.com/geo222222/Home_Sales.git
   cd Home_Sales
   ```

2. **Rename the Starter File**
   Rename the `Home_Sales_starter_code.ipynb` file to `Home_Sales.ipynb`.

3. **Data Loading**
   Import the necessary PySpark SQL functions and read the `home_sales_revised.csv` file into a Spark DataFrame.

4. **Creating a Temporary Table**
   Create a temporary table named `home_sales` from the DataFrame.

5. **Analysis Queries**
   Run the following queries using SparkSQL:
   - Average price of four-bedroom houses sold each year.
   - Average price of homes with three bedrooms and three bathrooms per year built.
   - Average price for homes with three bedrooms, three bathrooms, two floors, and a minimum of 2,000 square feet, per year built.
   - Average home price per view rating for homes priced at $350,000 or above, with runtime measured.

6. **Caching and Performance Comparison**
   - Cache the `home_sales` temporary table.
   - Validate that the table is cached.
   - Run the last query again on the cached data and compare runtimes.

7. **Data Partitioning**
   - Partition the home sales dataset by the `date_built` field and save it in parquet format.
   - Create a temporary table for the parquet data.
   - Execute the last query on the parquet table and compare runtimes.

8. **Uncaching the Table**
   - Uncache the `home_sales` temporary table and verify it is uncached.

## **Requirements**

The project consists of several requirements, each carrying specific points:
- Creation of Spark DataFrame and temporary table.
- Execution of multiple queries with correct rounding.
- Caching and validation of temporary tables.
- Performance measurement and comparison of queries.

## **Support and Resources**

For assistance, please reach out during class hours or office hours. Utilize learning assistants and tutors for any additional help with concepts covered in this project.

## **Submission**

Once completed, download your `Home_Sales.ipynb` file and upload it to this repository.

---

By following this README, you should be able to set up, execute, and analyze the home sales data using SparkSQL effectively. **Happy coding!**
