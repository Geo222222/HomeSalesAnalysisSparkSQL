# **Home Sales Analysis with SparkSQL**

## **Quickstart (Run in 5 Minutes) 🚀**

**For reviewers and quick demos:** Get the entire project running with Spark and Jupyter in under 5 minutes using Docker.

### **Prerequisites**
- [Docker](https://www.docker.com/get-started) and Docker Compose
- [Make](https://www.gnu.org/software/make/) (usually pre-installed on macOS/Linux, [install on Windows](https://gnuwin32.sourceforge.net/packages/make.htm))

### **Quick Start Steps**

1. **Clone and enter the repository:**
   ```bash
   git clone <repository-url>
   cd HomeSalesAnalysisSparkSQL
   ```

2. **Start the environment:**
   ```bash
   make run
   ```
   This will:
   - Pull the Jupyter PySpark Docker image
   - Start JupyterLab with Spark pre-configured
   - Mount your project files
   - Display the access URL

3. **Access JupyterLab:**
   - Open http://localhost:8888 in your browser
   - Navigate to `work/Home_Sales_NB.ipynb` to see the analysis
   - All project files are available in the `work/` directory

4. **Run performance benchmarks:**
   ```bash
   make bench
   ```
   This will:
   - Auto-detect or download the home sales CSV data
   - Run realistic queries comparing CSV vs Parquet performance
   - Test both cached and uncached scenarios
   - Generate `reports/benchmarks.csv` with timing results
   - Display a performance comparison table

5. **Stop the environment:**
   ```bash
   make stop
   ```

### **Benchmark Results**
The benchmark script tests 4 scenarios across realistic home sales queries:
- **CSV Uncached** (baseline)
- **CSV Cached** (in-memory caching)
- **Parquet Uncached** (columnar format)
- **Parquet Cached** (best performance)

Expected performance improvements:
- Caching: 2-5x faster
- Parquet format: 3-10x faster than CSV
- Combined (Parquet + Cache): 5-20x faster than baseline

### **Custom CSV Path**
If you have the CSV file locally, set the path:
```bash
export HOME_SALES_CSV=/path/to/your/home_sales_revised.csv
make bench
```

### **Available Commands**
- `make run` - Start Docker environment
- `make stop` - Stop and cleanup
- `make bench` - Run benchmarks (Docker or local)
- `make bench-local` - Force local Python execution
- `make clean` - Remove cache and reports
- `make help` - Show all commands

---

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
