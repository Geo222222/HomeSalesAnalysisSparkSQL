.PHONY: run stop bench bench-local clean help

# Default target
help:
	@echo "Available targets:"
	@echo "  run         - Start Docker containers and display access URL"
	@echo "  stop        - Stop and remove containers with volumes"
	@echo "  bench       - Run benchmark script (prefer container, fallback to local)"
	@echo "  bench-local - Run benchmark using local Python/PySpark"
	@echo "  clean       - Remove cache directories and generated reports"
	@echo "  help        - Show this help message"

# Start Docker containers and display access URL
run:
	@echo "Starting Jupyter with PySpark..."
	docker-compose up -d
	@echo ""
	@echo "🚀 Jupyter Lab is starting up..."
	@echo "📊 Access your notebook at: http://localhost:8888"
	@echo "📁 Your project files are mounted in /home/jovyan/work"
	@echo ""
	@echo "To stop the environment, run: make stop"

# Stop and remove containers with volumes
stop:
	@echo "Stopping and removing containers..."
	docker-compose down -v
	@echo "✅ Environment stopped and cleaned up"

# Run benchmark script (prefer container, fallback to local)
bench:
	@echo "Running performance benchmarks..."
	@if docker ps | grep -q home_sales_jupyter; then \
		echo "Using running Docker container..."; \
		docker exec home_sales_jupyter python /home/jovyan/work/scripts/bench_queries.py; \
	else \
		echo "Docker container not running, trying local execution..."; \
		$(MAKE) bench-local; \
	fi

# Run benchmark using local Python/PySpark
bench-local:
	@echo "Running benchmarks with local Python/PySpark..."
	@if command -v python3 >/dev/null 2>&1; then \
		python3 scripts/bench_queries.py; \
	elif command -v python >/dev/null 2>&1; then \
		python scripts/bench_queries.py; \
	else \
		echo "❌ Python not found. Please install Python or use 'make run' for Docker environment."; \
		exit 1; \
	fi

# Remove cache directories and generated reports
clean:
	@echo "Cleaning up cache and reports..."
	rm -rf _parquet_cache/
	rm -f reports/*.csv
	rm -rf .ipynb_checkpoints/
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "✅ Cleanup complete"
