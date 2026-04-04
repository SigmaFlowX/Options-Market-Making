import pandas
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

def load_datasets(db_url, ticker):
    pass



def main():
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    print(url)


if __name__ == "__main__":
    main()
