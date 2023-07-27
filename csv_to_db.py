import pandas as pd
from sqlalchemy import create_engine, Column, String, DateTime, Boolean,Integer
from sqlalchemy.ext.declarative import declarative_base
from dateutil.parser import parse
from dateutil import tz
# Replace 'sqlite:///store_data.db' with your desired database connection string
DATABASE_URL = 'sqlite:///store_data2.db'

Base = declarative_base()

class StoreStatus(Base):
    __tablename__ = 'store_status'
    store_id = Column(String, primary_key=True)
    timestamp_utc = Column(DateTime, primary_key=True)
    status = Column(Boolean, nullable=False)

class StoreBusinessHours(Base):
    __tablename__ = 'store_business_hours'
    store_id = Column(String, primary_key=True)
    day_of_week = Column(Integer, primary_key=True)
    start_time_local = Column(String)
    end_time_local = Column(String)

class StoreTimezone(Base):
    __tablename__ = 'store_timezone'
    store_id = Column(String, primary_key=True)
    timezone_str = Column(String, default='America/Chicago')

class ReportStatus(Base):
    __tablename__ = 'report_status'
    report_id = Column(String, primary_key=True)
    status = Column(Boolean, default=False)

def create_tables(engine):
    Base.metadata.create_all(engine)

def load_csv_data(csv_url):
    return pd.read_csv(csv_url)

def convert_to_isoformat(timestamp_str):
    try:
        # Parse the timestamp string and truncate microseconds to six digits
        timestamp_dt = parse(timestamp_str)
        # Assuming the input timestamps are in UTC, convert them to the desired format
        utc_tz = tz.tzutc()
        isoformat_str = timestamp_dt.astimezone(utc_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        return isoformat_str
    except Exception as e:
        print(f"Error converting timestamp: {e}")
        return None

def save_data_to_db(df, table_name):
    engine = create_engine(DATABASE_URL)
    # Convert the timestamp_utc column to ISO 8601 format with microseconds truncated to six digits
    if table_name == "store_status":
        df['timestamp_utc'] = df['timestamp_utc'].apply(convert_to_isoformat)
    df.to_sql(table_name, engine, if_exists='replace', index=False)


if __name__ == "__main__":
    # Load CSV data
    store_status_df = load_csv_data('data/store_status.csv')
    store_business_hours_df = load_csv_data('data/store_business_hours.csv')
    store_timezone_df = load_csv_data('data/store_timezone.csv')

    # Save data to the database
    engine = create_engine(DATABASE_URL)
    create_tables(engine)
    save_data_to_db(store_status_df, 'store_status')
    save_data_to_db(store_business_hours_df, 'store_business_hours')
    save_data_to_db(store_timezone_df, 'store_timezone')

    print("database is created")
