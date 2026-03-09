from api import get_data
from processing import processing_all
import schedule
import time


def run_pipeline():
    get_data()
    print("data collected! Starting processing... :D")
    processing_all()
    print("Processing done! ")


if __name__ == "__main__":
    # Simple, human-readable scheduling
    schedule.every(24).hours.do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(1)
