import time
import schedule
import subprocess
import logging
import sys


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout  
)


def run_spark_job_generic(spark_app_path):
    """
    Generic function để chạy bất kỳ Spark job nào.
    Args:
        spark_app_path (str): Path tới script Spark (e.g., /spark-apps/cleaner.py)
    """
    logging.info(f"Triggering Spark job: {spark_app_path}")
    
    command = [
        "docker", "exec", "spark-master",
        "/spark/bin/spark-submit", spark_app_path
    ]
    
    try:
        result = subprocess.run(
            command,
            check=True,         
            capture_output=True, 
            text=True,
            timeout=600  # 10 phút timeout
        )
        
        logging.info(f"Spark job {spark_app_path} completed successfully.")
        if result.stdout:
            logging.debug("SPARK JOB STDOUT:\n" + result.stdout[-500:])  # Last 500 chars
        
        if result.stderr:
            logging.warning(f"SPARK JOB STDERR:\n" + result.stderr[-500:])
        
    except subprocess.TimeoutExpired:
        logging.error(f"Spark job {spark_app_path} timed out after 600 seconds")
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Spark job {spark_app_path} failed with return code: {e.returncode}")
        if e.stdout:
            logging.error("STDOUT:\n" + e.stdout[-500:])
        if e.stderr:
            logging.error("STDERR:\n" + e.stderr[-500:])
        
    except FileNotFoundError:
        logging.error("Error: 'docker' command not found. Is the Docker client installed?")
        
    except Exception as e:
        logging.error(f"An unexpected error occurred in {spark_app_path}: {e}")


def run_cleaner_job():
    """Run cleaner job (raw JSON → clean Parquet)"""
    logging.info("=== Starting Cleaner Job ===")
    run_spark_job_generic("/spark-apps/cleaner.py")
    logging.info("=== Cleaner Job Completed ===")


def run_daily_aggregates_job():
    """Run daily aggregates job (clean Parquet → daily stats)"""
    logging.info("=== Starting Daily Aggregates Job ===")
    run_spark_job_generic("/spark-apps/batch_daily_aggregates.py")
    logging.info("=== Daily Aggregates Job Completed ===")


def run_dq_report_job():
    """Run data quality report job (DQ checks & metrics)"""
    logging.info("=== Starting Data Quality Report Job ===")
    run_spark_job_generic("/spark-apps/dq_report.py")
    logging.info("=== Data Quality Report Job Completed ===")


def run_hourly_aggregates_job():
    """Run hourly aggregates job (clean Parquet → hourly stats with feature engineering)"""
    logging.info("=== Starting Hourly Aggregates Job ===")
    run_spark_job_generic("/spark-apps/batch_hourly_aggregates.py")
    logging.info("=== Hourly Aggregates Job Completed ===")


def run_dq_hourly_job():
    """Run hourly data quality report job"""
    logging.info("=== Starting Hourly Data Quality Report Job ===")
    run_spark_job_generic("/spark-apps/dq_report_hourly.py")
    logging.info("=== Hourly Data Quality Report Job Completed ===")


# ===== BATCH LAYER SCHEDULING =====

# Cleaner job: every 2 minutes (TESTING mode) | every hour (PRODUCTION)
# Comment/uncomment as needed
schedule.every(2).minutes.do(run_cleaner_job)
# schedule.every().hour.do(run_cleaner_job)  # Production

# Daily aggregates job: 01:30 every day (after cleaner finishes around 01:00)
schedule.every().day.at("01:30").do(run_daily_aggregates_job)

# Data quality report job: 02:00 every day (after daily aggregates)
schedule.every().day.at("02:00").do(run_dq_report_job)

# Hourly aggregates job: every hour at minute 5
schedule.every().hour.at(":05").do(run_hourly_aggregates_job)

# Hourly data quality report job: every hour at minute 10
schedule.every().hour.at(":10").do(run_dq_hourly_job)


logging.info("Scheduler started.")
logging.info("Scheduled jobs:")
logging.info("  - Cleaner: every 2 minutes")
logging.info("  - Daily Aggregates: 01:30 every day")
logging.info("  - DQ Report: 02:00 every day")

logging.info("Running cleaner job once on startup to process existing data...")
# Chạy cleaner job ngay lập tức một lần khi khởi động
run_cleaner_job()
logging.info("Initial cleaner run finished. Waiting for the scheduled time...")

# Vòng lặp vô hạn để giữ cho script hoạt động và kiểm tra lịch trình
while True:
    # Kiểm tra xem có job nào đến giờ chạy chưa
    schedule.run_pending()
    # "Ngủ" 1 giây để tránh tốn CPU
    time.sleep(1)
