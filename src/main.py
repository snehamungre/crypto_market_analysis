from api import get_data
import schedule
import time


if __name__ == "__main__":
    # Simple, human-readable scheduling
    schedule.every(24).hours.do(get_data)
    schedule.every().day.at("08:30").do(get_data)

    while True:
        schedule.run_pending()
        time.sleep(1)
