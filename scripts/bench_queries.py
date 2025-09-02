#!/usr/bin/env python3
"""
Home Sales Performance Benchmark Script

This script runs realistic home sales queries and compares performance across:
- CSV uncached/cached
- Parquet uncached/cached

Auto-detects CSV file location and outputs results to reports/benchmarks.csv
"""

import os
import sys
import time
import csv
from pathlib import Path

# Handle pandas import gracefully
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("⚠️  pandas not available - will use basic CSV output")

def find_csv_file():
    """Auto-detect CSV file location with multiple fallback strategies"""
    
    # Strategy 1: Check environment variable
    env_path = os.environ.get('HOME_SALES_CSV')
    if env_path and os.path.exists(env_path):
        print(f"📁 Found CSV via HOME_SALES_CSV: {env_path}")
        return env_path
    
    # Strategy 2: Check common local paths
    local_paths = [
        'home_sales_revised.csv',
        'data/home_sales_revised.csv',
        '../home_sales_revised.csv',
        './home_sales_revised.csv'
    ]
    
    for path in local_paths:
        if os.path.exists(path):
            print(f"📁 Found CSV locally: {path}")
            return path
    
    # Strategy 3: Download from AWS S3 (same URL as notebook)
    print("📥 CSV not found locally, downloading from AWS S3...")
    try:
        from pyspark import SparkFiles
        from pyspark.sql import SparkSession
        
        # Create minimal Spark session for download
        spark = SparkSession.builder.appName("CSVDownload").getOrCreate()
        url = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"
        spark.sparkContext.addFile(url)
        csv_path = SparkFiles.get("home_sales_revised.csv")
        
        if os.path.exists(csv_path):
            print(f"📁 Downloaded CSV to: {csv_path}")
            return csv_path
            
    except Exception as e:
        print(f"❌ Failed to download CSV: {e}")
    
    # Strategy 4: Fallback error
    print("❌ Could not locate home_sales_revised.csv")
    print("💡 Try setting HOME_SALES_CSV environment variable to the file path")
    print("💡 Or place the file in the current directory")
    sys.exit(1)

def setup_spark():
    """Initialize Spark session with optimized settings"""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, avg, count, round as spark_round
        
        spark = SparkSession.builder \
            .appName("HomeSalesBenchmark") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
            
        return spark, col, avg, count, spark_round
        
    except ImportError:
        print("❌ PySpark not available. Please install PySpark or use Docker environment.")
        print("💡 Run: pip install pyspark")
        print("💡 Or use: make run")
        sys.exit(1)

def load_data(spark, csv_path):
    """Load CSV data into Spark DataFrame"""
    print(f"📊 Loading data from {csv_path}...")
    
    df = spark.read.csv(csv_path, sep=",", header=True, inferSchema=True)
    row_count = df.count()
    print(f"✅ Loaded {row_count:,} rows")
    
    # Create temporary view
    df.createOrReplaceTempView("home_sales")
    return df

def run_benchmark_queries(spark, col, avg, count, spark_round):
    """Run realistic home sales analysis queries"""
    
    queries = {
        "avg_price_4bed_by_year": """
            SELECT date_built, ROUND(AVG(price), 2) as avg_price
            FROM home_sales 
            WHERE bedrooms = 4 
            GROUP BY date_built 
            ORDER BY date_built
        """,
        
        "avg_price_3bed_3bath_by_year": """
            SELECT date_built, ROUND(AVG(price), 2) as avg_price
            FROM home_sales 
            WHERE bedrooms = 3 AND bathrooms = 3 
            GROUP BY date_built 
            ORDER BY date_built
        """,
        
        "avg_price_luxury_homes": """
            SELECT date_built, ROUND(AVG(price), 2) as avg_price
            FROM home_sales 
            WHERE bedrooms = 3 AND bathrooms = 3 AND floors = 2 AND sqft_living >= 2000
            GROUP BY date_built 
            ORDER BY date_built
        """,
        
        "avg_price_by_view_rating": """
            SELECT view, ROUND(AVG(price), 2) as avg_price
            FROM home_sales 
            WHERE price >= 350000
            GROUP BY view 
            ORDER BY view DESC
        """
    }
    
    results = {}
    
    for query_name, sql in queries.items():
        print(f"🔍 Running {query_name}...")
        start_time = time.time()
        
        result_df = spark.sql(sql)
        # Force execution by collecting results
        result_df.collect()
        
        end_time = time.time()
        execution_time = end_time - start_time
        results[query_name] = execution_time
        print(f"   ⏱️  {execution_time:.3f} seconds")
    
    return results

def benchmark_scenario(spark, df, scenario_name, setup_func=None):
    """Benchmark a specific scenario (CSV cached/uncached, Parquet cached/uncached)"""
    print(f"\n🚀 Benchmarking: {scenario_name}")
    print("=" * 50)

    # Setup for this scenario (caching, parquet conversion, etc.)
    if setup_func:
        setup_func(spark, df)

    # Run all queries and collect timings
    _, col, avg, count, spark_round = setup_spark()
    results = run_benchmark_queries(spark, col, avg, count, spark_round)

    # Add scenario name to results
    scenario_results = {f"{scenario_name}_{query}": time_val for query, time_val in results.items()}
    return scenario_results

def setup_csv_cached(spark, df):
    """Setup for CSV cached scenario"""
    print("💾 Caching CSV data...")
    spark.catalog.cacheTable("home_sales")
    # Trigger caching by running a simple query
    spark.sql("SELECT COUNT(*) FROM home_sales").collect()
    print("✅ CSV data cached")

def setup_parquet_uncached(spark, df):
    """Setup for Parquet uncached scenario"""
    parquet_path = "_parquet_cache/home_sales_partitioned"

    print(f"💿 Writing data to Parquet format: {parquet_path}")
    # Partition by date_built for better query performance
    df.write.mode("overwrite").partitionBy("date_built").parquet(parquet_path)

    # Read parquet data and create new temp view
    parquet_df = spark.read.parquet(parquet_path)
    parquet_df.createOrReplaceTempView("home_sales")
    print("✅ Parquet data loaded (uncached)")

def setup_parquet_cached(spark, df):
    """Setup for Parquet cached scenario"""
    setup_parquet_uncached(spark, df)
    print("💾 Caching Parquet data...")
    spark.catalog.cacheTable("home_sales")
    # Trigger caching
    spark.sql("SELECT COUNT(*) FROM home_sales").collect()
    print("✅ Parquet data cached")

def save_results(all_results):
    """Save benchmark results to CSV file"""

    # Ensure reports directory exists
    os.makedirs("reports", exist_ok=True)
    output_file = "reports/benchmarks.csv"

    print(f"\n💾 Saving results to {output_file}")

    if HAS_PANDAS:
        # Use pandas for nice formatting
        df_results = pd.DataFrame([all_results])
        df_results.to_csv(output_file, index=False)
    else:
        # Fallback to basic CSV writing
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=all_results.keys())
            writer.writeheader()
            writer.writerow(all_results)

    print(f"✅ Results saved to {output_file}")

def display_comparison_table(all_results):
    """Display a formatted comparison table of results"""

    print("\n📊 PERFORMANCE COMPARISON")
    print("=" * 80)

    # Extract scenarios and queries
    scenarios = ["csv_uncached", "csv_cached", "parquet_uncached", "parquet_cached"]
    queries = ["avg_price_4bed_by_year", "avg_price_3bed_3bath_by_year",
               "avg_price_luxury_homes", "avg_price_by_view_rating"]

    # Print header
    print(f"{'Query':<30} {'CSV':<12} {'CSV':<12} {'Parquet':<12} {'Parquet':<12}")
    print(f"{'Name':<30} {'Uncached':<12} {'Cached':<12} {'Uncached':<12} {'Cached':<12}")
    print("-" * 80)

    # Print results for each query
    for query in queries:
        row = f"{query:<30}"
        for scenario in scenarios:
            key = f"{scenario}_{query}"
            time_val = all_results.get(key, 0.0)
            row += f"{time_val:<12.3f}"
        print(row)

    print("-" * 80)

    # Calculate and display totals
    totals_row = f"{'TOTAL':<30}"
    for scenario in scenarios:
        total_time = sum(all_results.get(f"{scenario}_{query}", 0.0) for query in queries)
        totals_row += f"{total_time:<12.3f}"
    print(totals_row)

    # Performance insights
    print("\n💡 INSIGHTS:")
    csv_uncached_total = sum(all_results.get(f"csv_uncached_{query}", 0.0) for query in queries)
    csv_cached_total = sum(all_results.get(f"csv_cached_{query}", 0.0) for query in queries)
    parquet_uncached_total = sum(all_results.get(f"parquet_uncached_{query}", 0.0) for query in queries)
    parquet_cached_total = sum(all_results.get(f"parquet_cached_{query}", 0.0) for query in queries)

    if csv_cached_total > 0:
        cache_speedup = csv_uncached_total / csv_cached_total
        print(f"   🚀 CSV Caching speedup: {cache_speedup:.1f}x faster")

    if parquet_uncached_total > 0:
        parquet_speedup = csv_uncached_total / parquet_uncached_total
        print(f"   💿 Parquet format speedup: {parquet_speedup:.1f}x faster than CSV")

    if parquet_cached_total > 0:
        best_speedup = csv_uncached_total / parquet_cached_total
        print(f"   ⚡ Best case (Parquet + Cache): {best_speedup:.1f}x faster than baseline")

def main():
    """Main benchmark execution function"""
    print("🏠 Home Sales Performance Benchmark")
    print("=" * 50)

    # Find and validate CSV file
    csv_path = find_csv_file()

    # Setup Spark
    spark, col, avg, count, spark_round = setup_spark()

    try:
        # Load initial data
        df = load_data(spark, csv_path)

        # Run all benchmark scenarios
        all_results = {}

        # Scenario 1: CSV Uncached (baseline)
        scenario_results = benchmark_scenario(spark, df, "csv_uncached")
        all_results.update(scenario_results)

        # Scenario 2: CSV Cached
        scenario_results = benchmark_scenario(spark, df, "csv_cached", setup_csv_cached)
        all_results.update(scenario_results)

        # Uncache before parquet tests
        spark.catalog.uncacheTable("home_sales")

        # Scenario 3: Parquet Uncached
        scenario_results = benchmark_scenario(spark, df, "parquet_uncached", setup_parquet_uncached)
        all_results.update(scenario_results)

        # Scenario 4: Parquet Cached
        scenario_results = benchmark_scenario(spark, df, "parquet_cached", setup_parquet_cached)
        all_results.update(scenario_results)

        # Save and display results
        save_results(all_results)
        display_comparison_table(all_results)

        print("\n✅ Benchmark completed successfully!")
        print(f"📊 Results saved to reports/benchmarks.csv")

    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        sys.exit(1)

    finally:
        # Cleanup
        try:
            spark.catalog.uncacheTable("home_sales")
        except:
            pass
        spark.stop()

if __name__ == "__main__":
    main()
