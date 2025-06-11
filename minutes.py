from datetime import datetime, timedelta
import MetaTrader5 as mt5
import pandas as pd
import time
import sqlite3 # Changed from pyodbc to sqlite3

# Set up pandas display options
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1500)

# --- Configuration ---
# SQLite database file path on the Z: drive (for shared folders)
DATABASE_FILE = r"Z:\Users\swift\Desktop\BTCUSDminutes.db" # Changed to SQLite file on Z: drive
TABLE_NAME = "BTCUSDminutes"  # Table name for minute data
INITIAL_CANDLES = 90000  # Number of candles to fetch at startup (approx 62.5 days of M1 data)

def initialize_mt5():
    """Initialize connection to MetaTrader 5 without GUI"""
    if not mt5.initialize(portable=True):  # Run without GUI
        print("Initialize() failed, error code =", mt5.last_error())
        return False
    print("MetaTrader 5 connected successfully in headless mode")
    return True

def create_database_connection():
    """Create a connection to SQLite database"""
    try:
        conn = sqlite3.connect(DATABASE_FILE) # Use sqlite3.connect
        cursor = conn.cursor()
        return conn, cursor
    except sqlite3.Error as e: # Catch sqlite3 errors
        print(f"Database connection error: {e}")
        return None, None

def create_table(conn, cursor, recreate=True):
    """Create table for BTC USD minute data with proper error handling"""
    try:
        # Drop table if it exists and recreate is True
        if recreate:
            cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}") # SQLite syntax for dropping table
            conn.commit()
            print(f"Table {TABLE_NAME} dropped (if it existed)")

        # Create table with added candle_type and range columns
        # Column names don't need escaping in SQLite unless they are keywords
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            time TEXT PRIMARY KEY, -- SQLite uses TEXT for DATETIME
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            tick_volume INTEGER,
            spread INTEGER,
            real_volume INTEGER,
            candle_type TEXT,
            range REAL
        )
        """)
        conn.commit()
        print(f"Table {TABLE_NAME} created")
        return True
    except sqlite3.Error as e: # Catch sqlite3 errors
        print(f"Error creating table: {e}")
        return False

def determine_candle_type(open_price, close_price):
    """Determine if a candle is bullish or bearish"""
    if close_price > open_price:
        return "bullish"
    elif close_price < open_price:
        return "bearish"
    else:
        return "neutral"  # When open equals close

def calculate_candle_range(high, low):
    """Calculate the range of a candle (high minus low)"""
    return high - low

def format_data(rates_frame):
    """Process the raw MT5 data frame"""
    if rates_frame is None or len(rates_frame) == 0:
        return pd.DataFrame()

    # Convert time in seconds to datetime format
    rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')

    # Remove 2 hours (timezone adjustment) - This assumes your MT5 server time
    # is 2 hours ahead of the local time you want to store.
    # Adjust this if your local/desired timezone offset is different.
    rates_frame['time'] = rates_frame['time'] - pd.Timedelta(hours=2)

    # Convert datetime objects to string format for SQLite TEXT column
    rates_frame['time'] = rates_frame['time'].dt.strftime('%Y-%m-%d %H:%M:%S') # Format for SQLite TEXT

    # Calculate candle type and range
    rates_frame['candle_type'] = rates_frame.apply(
        lambda row: determine_candle_type(row['open'], row['close']), axis=1
    )
    rates_frame['range'] = rates_frame.apply(
        lambda row: calculate_candle_range(row['high'], row['low']), axis=1
    )

    return rates_frame

def insert_data(conn, cursor, data_frame):
    """Insert data into the database"""
    if data_frame is None or len(data_frame) == 0:
        print("No data to insert")
        return 0

    rows_inserted = 0

    for _, row in data_frame.iterrows():
        try:
            # Check if record already exists by primary key ('time' column)
            # SQLite does not use dbo. prefix, and column names don't need escaping in the query itself.
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE time = ?",
                           (row['time'],))
            if cursor.fetchone()[0] == 0: # If record does not exist
                cursor.execute(f'''
                INSERT INTO {TABLE_NAME}
                (time, open, high, low, close, tick_volume, spread, real_volume, candle_type, range)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['time'],
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(row['tick_volume']),
                    int(row['spread']),
                    int(row['real_volume']),
                    row['candle_type'],
                    float(row['range'])
                ))
                rows_inserted += 1

                # Commit every 100 rows to avoid large transactions
                if rows_inserted % 100 == 0:
                    conn.commit()
                    print(f"Committed {rows_inserted} rows so far...")
        except sqlite3.Error as e: # Catch sqlite3 errors
            # Handle cases where a record might be inserted by another process or committed
            # right after check. SQLite's IntegrityError for PRIMARY KEY is common.
            if "UNIQUE constraint failed" in str(e):
                # print(f"Skipping duplicate entry for time: {row['time']}")
                pass # This is expected for existing candles, so we just skip it
            else:
                print(f"Error inserting data: {e}")
                print(f"Problem row: {row}")

    try:
        # Final commit for remaining rows
        conn.commit()
        print(f"Total rows inserted: {rows_inserted}")
        return rows_inserted
    except sqlite3.Error as e: # Catch sqlite3 errors
        print(f"Error committing data: {e}")
        return 0


def fetch_initial_historical_data():
    """Fetch large amount of historical data"""
    print(f"Fetching initial historical data ({INITIAL_CANDLES} candles)...")
    # Changed timeframe to M1 for minute data
    rates = mt5.copy_rates_from_pos("BTCUSD", mt5.TIMEFRAME_M1, 0, INITIAL_CANDLES)

    if rates is not None and len(rates) > 0:
        rates_frame = pd.DataFrame(rates)
        rates_frame = format_data(rates_frame)

        # Exclude the last candle (potentially unfinished, current minute)
        historical_data = rates_frame.iloc[:-1]
        print(f"Processed {len(historical_data)} historical candles (excluded last unfinished candle)")

        return historical_data
    else:
        print("Error: No historical data returned from MT5")
        return pd.DataFrame()

def fetch_latest_data():
    """Fetch 2 latest candles, return only the completed one (previous minute)"""
    # Timeframe set to M1 for minute data
    # We fetch 2 candles: the current (incomplete) minute and the previous (completed) minute.
    rates = mt5.copy_rates_from_pos("BTCUSD", mt5.TIMEFRAME_M1, 0, 2)

    if rates is not None and len(rates) >= 2: # Ensure we got at least two candles
        rates_frame = pd.DataFrame(rates)
        rates_frame = format_data(rates_frame)

        # The latest data returned by copy_rates_from_pos(..., 0, count) is ordered
        # from oldest to newest. So, if count=2, index 0 is the previous minute's
        # completed candle, and index 1 is the current (incomplete) minute's candle.
        # We want to store the completed one, which is at index 0.
        latest_completed_candle = rates_frame.iloc[0:1]
        return latest_completed_candle, rates_frame

    print("Warning: Could not fetch at least 2 latest minute candles from MT5.")
    return pd.DataFrame(), pd.DataFrame()

def calculate_seconds_to_next_fetch():
    """Calculate seconds until 5 seconds after the next minute begins"""
    current_time = datetime.now()
    # Calculate the beginning of the next minute
    next_minute = current_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
    # Add 5 seconds to the beginning of the next minute
    fetch_time = next_minute + timedelta(seconds=5)
    wait_seconds = (fetch_time - current_time).total_seconds()
    # Ensure wait_seconds is not negative if the current time is already past the fetch time.
    # If it's already past the 5-second mark of the current minute, wait for the next minute.
    if wait_seconds < 0:
        wait_seconds = (next_minute + timedelta(minutes=1) + timedelta(seconds=5) - current_time).total_seconds()
    
    return int(wait_seconds)


def main():
    print("Starting MetaTrader5 BTC minute data collection with enhanced candle analysis (SQLite3 on Z:)...")
    # Initialize MT5
    if not initialize_mt5():
        return

    # Create database connection
    conn, cursor = create_database_connection()
    if not conn or not cursor:
        print("Failed to connect to database, exiting.")
        mt5.shutdown()
        return

    try:
        # Create table, dropping if it exists
        if not create_table(conn, cursor, recreate=True):
            print("Failed to create or access table, exiting.")
            return

        # Fetch and store initial historical data
        print("Fetching initial historical data...")
        historical_data = fetch_initial_historical_data()
        if not historical_data.empty:
            insert_data(conn, cursor, historical_data)
            print(f"Added {len(historical_data)} initial candles to database")
        else:
            print("Failed to fetch initial historical data.")
            print("Proceeding with real-time data collection anyway...")


        # Main loop for continuous updates
        print("Starting continuous data collection...")
        while True:
            # Calculate seconds until the next fetch time (5 seconds after the minute)
            seconds_to_next_fetch = calculate_seconds_to_next_fetch()

            print(f"Waiting {seconds_to_next_fetch} seconds until next fetch (5 seconds after the minute)...")
            time.sleep(seconds_to_next_fetch)

            # Fetch latest data (we expect 2 candles, store the completed one)
            current_time = datetime.now() # Get current time before fetching
            latest_data, full_data = fetch_latest_data()

            if not latest_data.empty:
                # Store the completed candle to database
                insert_data(conn, cursor, latest_data)

                # Display information for monitoring
                print("\n--- Data updated at:", current_time, "---")
                print("Latest stored candle (completed minute):")
                print(latest_data[['time', 'open', 'high', 'low', 'close', 'candle_type', 'range']])
            else:
                print(f"\n--- No new data available (or less than 2 candles fetched) at: {current_time} ---")


    except KeyboardInterrupt:
        print("\nScript terminated by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Close database connection and MT5
        if conn:
            conn.close()
        mt5.shutdown()
        print("MT5 connection closed and database connection closed")

if __name__ == "__main__":
    main()