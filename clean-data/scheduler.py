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

def run_spark_job():
    """
    Hàm này thực thi job Spark bằng cách gọi lệnh 'docker exec'
    nhắm vào container 'spark-master'.
    """
    logging.info("Triggering Spark job...")
    
    command = [
        "docker", "exec", "spark-master",
        "/spark/bin/spark-submit", "/spark-apps/cleaner.py"
    ]
    
    try:
        result = subprocess.run(
            command,
            check=True,         
            capture_output=True, 
            text=True           
        )
        
        logging.info("Spark job completed successfully.")
        logging.info("SPARK JOB STDOUT:\n" + result.stdout)
        
        if result.stderr:
            logging.warning("SPARK JOB STDERR:\n" + result.stderr)
            
    except subprocess.CalledProcessError as e:
        logging.error("Spark job failed with return code: " + str(e.returncode))
        logging.error("SPARK JOB STDOUT:\n" + e.stdout)
        logging.error("SPARK JOB STDERR:\n" + e.stderr)
        
    except FileNotFoundError:
        logging.error("Error: 'docker' command not found. Is the Docker client installed in this container?")
        
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

# --- Lập lịch ---

# Dành cho MÔI TRƯỜNG CHẠY THẬT (PRODUCTION):
# Chạy job mỗi ngày vào lúc 01:00 (sáng)
#schedule.every().day.at("01:00").do(run_spark_job)

# Dành cho MÔI TRƯỜNG KIỂM THỬ (TESTING):
# Để kiểm tra nhanh, hãy bỏ comment dòng dưới và comment dòng trên.
# Nó sẽ chạy job mỗi 2 phút một lần.
schedule.every(2).minutes.do(run_spark_job)



logging.info("Scheduler started.")
logging.info("Running job once on startup to process existing data...")
# Chạy job ngay lập tức một lần khi khởi động
run_spark_job()
logging.info("Initial job run finished. Waiting for the scheduled time...")

# Vòng lặp vô hạn để giữ cho script hoạt động và kiểm tra lịch trình
while True:
    # Kiểm tra xem có job nào đến giờ chạy chưa
    schedule.run_pending()
    # "Ngủ" 1 giây để tránh tốn CPU
    time.sleep(1)
