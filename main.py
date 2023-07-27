from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, String, DateTime, Boolean,Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from pytz import timezone
from dateutil import tz
from dateutil.parser import parse
from multiprocessing import Pool, cpu_count
import pandas as pd
import os
import secrets

# Initialize FastAPI app
app = FastAPI(
    docs_url=None,
    redoc_url=None,
    title="Loop API v1",
    description="""
    This API allows users to retrieve the status of the report or the complete CSV file containing restaurant uptime and downtime data.
    
    ## Usage
    - To trigger a new report, make a POST request to `/trigger_report/`.
    - To check the status of a report, make a GET request to `/get_report/{report_id}/`.
    
    ## Report Status
    - When a new report is triggered, the API will return a unique `report_id`.
    - You can use this `report_id` to check the status of the report and download the CSV file once it's complete.
    
    ## CSV File Format
    The CSV file contains the following columns:
    - `store_id`: The unique identifier of the store.
    - `uptime_last_hour`: The percentage of uptime in the last hour.
    - `uptime_last_day`: The percentage of uptime in the last 24 hours.
    - `uptime_last_week`: The percentage of uptime in the last 7 days.
    - `downtime_last_hour`: The percentage of downtime in the last hour.
    - `downtime_last_day`: The percentage of downtime in the last 24 hours.
    - `downtime_last_week`: The percentage of downtime in the last 7 days.
    
    ## Note
    - The API uses background tasks to generate the reports, so you can check the status and download the file later.
    - Reports are generated for each store based on their poll data and business hours.
    - If the store has missing data, the report will indicate that in the corresponding fields.
    """,
    version="1.0",
)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/api/v1/docs", include_in_schema=False)
def overridden_swagger():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Loop API v1", swagger_favicon_url="/static/favicon.ico")

@app.get("/api/v1/redoc", include_in_schema=False)
def overridden_redoc():
    return get_redoc_html(openapi_url="/openapi.json", title="Loop API v1", redoc_favicon_url="/static/favicon.ico")


# Database configurations
DATABASE_URL = 'sqlite:///store_data.db'
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define SQLAlchemy models
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

# Create the database tables
Base.metadata.create_all(bind=engine)

def generate_unique_report_id():
    # Function to generate a unique report_id
    while True:
        report_id = secrets.token_urlsafe(6)
        with SessionLocal() as db:
            existing_report = db.query(ReportStatus).filter_by(report_id=report_id).first()
        if not existing_report:
            return report_id

def convert_to_isoformat(timestamp_str):
        # Parse the timestamp string and truncate microseconds to six digits
        timestamp_dt = parse(timestamp_str)
        # Assuming the input timestamps are in UTC, convert them to the desired format
        isoformat_str = timestamp_dt.astimezone(timezone('UTC')).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        return isoformat_str

# Convert local business hours to UTC
def convert_local_to_utc(row, df_timezones):
    store_id = row['store_id']
    timezone_info = df_timezones.loc[df_timezones['store_id'] == store_id, 'timezone_str']
    if not timezone_info.empty:
        tz_str = timezone_info.values[0]
        tz_local = timezone(tz_str)
        dt_local = datetime.strptime(row['start_time_local'], '%H:%M:%S')
        dt_local = tz_local.localize(dt_local)
        dt_utc = dt_local.astimezone(timezone('UTC'))
        return dt_utc.strftime('%H:%M:%S')
    else:
        return None  # or any default value you prefer for missing timezones

def preprocess_polls_data():
    # Fetch data from the database using SQL queries
    with engine.connect() as conn:
        df_status = pd.read_sql_table('store_status', conn)
        df_business_hours = pd.read_sql_table('store_business_hours', conn)
        df_timezones = pd.read_sql_table('store_timezone', conn)

    # Convert timestamps to datetime objects
    df_status['timestamp_utc'] = pd.to_datetime(df_status['timestamp_utc'].apply(convert_to_isoformat))
   

    if not df_timezones.empty:
        # Filter out rows with missing store_id in df_timezones
        df_business_hours = df_business_hours[df_business_hours['store_id'].isin(df_timezones['store_id'])]

        # Convert local business hours to UTC
        df_business_hours['start_time_utc'] = df_business_hours.apply(convert_local_to_utc, args=(df_timezones,), axis=1)

    return df_status, df_business_hours, df_timezones

def calculate_store_stats(store_id, df_polls, df_business_hours):
    df_store_polls = df_polls[df_polls['store_id'] == store_id]
    df_store_hours = df_business_hours[df_business_hours['store_id'] == store_id]

    # Extrapolate uptime and downtime for the entire time interval
    total_duration = 24 * 60  # 24 hours in minutes
    uptime_duration = df_store_polls[df_store_polls['status'] == 'active']['timestamp_utc'].diff().sum().total_seconds() / 60
    downtime_duration = total_duration - uptime_duration

    # Get the time interval for the report
    interval_last_hour = 60
    interval_last_day = 24 * 60
    interval_last_week = 7 * 24 * 60

    # Return the statistics for the store
    return {
        'store_id': store_id,
        'uptime_last_hour': (uptime_duration / total_duration) * interval_last_hour,
        'uptime_last_day': (uptime_duration / total_duration) * interval_last_day,
        'uptime_last_week': (uptime_duration / total_duration) * interval_last_week,
        'downtime_last_hour': (downtime_duration / total_duration) * interval_last_hour,
        'downtime_last_day': (downtime_duration / total_duration) * interval_last_day,
        'downtime_last_week': (downtime_duration / total_duration) * interval_last_week
    }

def compute_and_save_report(report_id):
    df_polls, df_business_hours, df_timezones = preprocess_polls_data()

    num_processes = cpu_count()  # Number of CPU cores available
    store_ids = df_polls['store_id'].unique()

    with Pool(processes=num_processes) as pool:
        results = pool.starmap(calculate_store_stats, [(store_id, df_polls, df_business_hours) for store_id in store_ids])

    # Create the report dataframe from the results
    report_df = pd.DataFrame(results)

    if not os.path.exists('report_data'):
        os.makedirs('report_data')

    # Save the report to a CSV file
    report_file_path = f"report_data/report_{report_id}.csv"
    report_df.to_csv(report_file_path, index=False)

    # Update the report status to complete
    with SessionLocal() as db:
        # Check if the report_id already exists in the database
        existing_report_status = db.query(ReportStatus).filter_by(report_id=report_id).first()

        if existing_report_status:
            # If the report_id exists, update its status
            existing_report_status.status = True
            db.commit()
        else:
            # If the report_id doesn't exist, insert a new record
            report_status = ReportStatus(report_id=report_id, status=True)
            db.add(report_status)
            db.commit()


# API to trigger report generation
@app.post("/trigger_report/", tags=["Reports"], response_model=dict)
async def trigger_report(background_tasks: BackgroundTasks):
    # Generate a unique report_id
    report_id = generate_unique_report_id()

    # Insert the report_id into the database with status=False (report is running)
    with SessionLocal() as db:
        report_status = ReportStatus(report_id=report_id, status=False)
        db.add(report_status)
        db.commit()

    # Run the compute_and_save_report function in the background with the generated report_id
    background_tasks.add_task(compute_and_save_report, report_id)

    return {"report_id": report_id}

@app.get("/get_report/{report_id}/", tags=["Reports"], response_model=dict)
async def get_report(report_id: str):
    # Check if the report exists in the database
    with SessionLocal() as db:
        report_status = db.query(ReportStatus).filter(ReportStatus.report_id == report_id).first()

    # Check if the corresponding report CSV file exists in the report folder
    if not os.path.exists('report_data'):
        os.makedirs('report_data')
    report_file_path = f"report_data/report_{report_id}.csv"

    if os.path.exists(report_file_path):
        # If the report CSV file exists, update the database status to True (report is complete) if needed
        if report_status is None:
            with SessionLocal() as db:
                report_status = ReportStatus(report_id=report_id, status=True)
                db.add(report_status)
                db.commit()
            return {"status": "Complete", "csv_file": report_file_path}
        else:
            return {"status": "Complete", "csv_file": report_file_path}
    else:
        # If the report CSV file is missing, update the database status to False (report is not complete)
        if report_status is not None:
            with SessionLocal() as db:
                report_status.status = False
                db.commit()
        else:
            raise HTTPException(status_code=404, detail="Report not found")

        return {"status": "Running"}
