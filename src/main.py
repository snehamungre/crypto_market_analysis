from analysis import analysis
from api import get_data
from processing import process_new
import schedule
import time


def run_pipeline():
    get_data()
    print("data collected! Starting processing... :D")
    process_new()
    print("Processing done!")
    analysis()


if __name__ == "__main__":
    schedule.every(24).hours.do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(1)
