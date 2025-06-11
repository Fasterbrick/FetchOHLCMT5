from datetime import datetime, timedelta
import MetaTrader5 as mt5
import pandas as pd
import time
import sqlite3 # Changed from pyodbc to sqlite3

# Set up pandas display options
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1500)

# --- Configuration ---
# SQLite database file path
DATABASE_FILE = "BTCUSDdaily.db" # Changed to SQLite file
TABLE_NAME = "BTCUSDdaily"  # Table name for daily data
INITIAL_CANDLES = 5000  # Number of candles to fetch at startup

def initialize_mt5():
    """Initialize connection to MetaTrader 5 without GUI"""
    # Using portable=True for potential headless operation
    if not mt5.initialize(portable=True):
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
    """Create table for BTCUSD daily data with proper error handling"""
    try:
        # Drop table if it exists and recreate is True
        if recreate:
            cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}") # SQLite syntax for dropping table
            conn.commit()
            print(f"Table {TABLE_NAME} dropped (if it existed)")

        # Create table with added candle_type and range columns
        # SQLite data types: TEXT for DATETIME, REAL for FLOAT, INTEGER for INT/BIGINT
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            time TEXT PRIMARY KEY,
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

    # Remove 2 hours (timezone adjustment) - Adjust if your server/local timezone offset is different
    rates_frame['time'] = rates_frame['time'] - pd.Timedelta(hours=2)
    
    # Convert datetime objects to string format for SQLite TEXT column
    rates_frame['time_dt'] = rates_frame['time'] # Keep a datetime version for calculations if needed elsewhere
    rates_frame['time'] = rates_frame['time'].dt.strftime('%Y-%m-%d %H:%M:%S')


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
            if "UNIQUE constraint failed" in str(e):
                # This can happen if a candle was already inserted by a previous attempt or run
                # print(f"Skipping duplicate entry for time: {row['time']}") 
                pass # Skip duplicate entries silently
            else:
                print(f"Error inserting data: {e}")
                print(f"Problem row: {row}")
    try:
        conn.commit() # Final commit for any remaining rows
        if rows_inserted > 0 :
            print(f"Total rows inserted: {rows_inserted}")
        return rows_inserted
    except sqlite3.Error as e:
        print(f"Error committing data: {e}")
        return 0


def fetch_initial_historical_data():
    """Fetch large amount of historical data"""
    print(f"Fetching initial historical data ({INITIAL_CANDLES} candles)...")
    # Timeframe set to D1 for daily data
    # Fetching INITIAL_CANDLES + 1 to exclude the current day's potentially unfinished candle
    rates = mt5.copy_rates_from_pos("BTCUSD", mt5.TIMEFRAME_D1, 0, INITIAL_CANDLES + 1)


    if rates is not None and len(rates) > 0:
        rates_frame = pd.DataFrame(rates)
        rates_frame = format_data(rates_frame)

        # Exclude the last candle (current day's potentially unfinished)
        # Use iloc up to the second-to-last row (all completed candles)
        historical_data = rates_frame.iloc[:-1]
        print(f"Processed {len(historical_data)} historical candles (excluded last unfinished candle)")

        return historical_data
    else:
        print("Error: No historical data returned from MT5 for initial fetch")
        return pd.DataFrame()

def fetch_latest_data():
    """Fetch 2 latest daily candles, return only the completed one (yesterday's)"""
    # Timeframe set to D1 for daily data
    # Fetch the last 2 candles: previous day (completed) and current day (incomplete)
    rates = mt5.copy_rates_from_pos("BTCUSD", mt5.TIMEFRAME_D1, 0, 2)

    if rates is not None and len(rates) >= 2: # Ensure we got at least two candles
        rates_frame = pd.DataFrame(rates)
        rates_frame = format_data(rates_frame)

        # copy_rates_from_pos returns data ordered oldest to newest.
        # For count=2 from start_pos=0:
        # rates_frame.iloc[0] is the previous completed day's candle.
        # rates_frame.iloc[1] is the current, incomplete day's candle.
        # We want to store the completed one.
        latest_completed_data = rates_frame.iloc[0:1]
        return latest_completed_data
    
    print("Warning: Could not fetch at least 2 latest daily candles from MT5")
    return pd.DataFrame()

def calculate_seconds_to_next_fetch():
    """Calculate seconds until 5 seconds after the next day begins"""
    current_time = datetime.now()
    # Calculate the beginning of the next day
    next_day = current_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    # Add 5 seconds to the beginning of the next day
    fetch_time = next_day + timedelta(seconds=5)
    wait_seconds = (fetch_time - current_time).total_seconds()
    
    # Ensure wait_seconds is not negative if the current time is already past the fetch time
    if wait_seconds < 0:
        # If past the 5-second mark for today, target 5 seconds past the *next* actual day.
        wait_seconds = (next_day + timedelta(days=1) + timedelta(seconds=5) - current_time).total_seconds()
    
    return int(wait_seconds)


def main():
    print("Starting MetaTrader5 BTC daily data collection to SQLite with enhanced candle analysis...")
    # Initialize MT5
    if not initialize_mt5():
        return

    # Create database connection
    conn, cursor = create_database_connection()
    if not conn or not cursor:
        print("Failed to connect to SQLite database, exiting.")
        if mt5.terminal_state()[0]: # Check if MT5 is initialized before shutting down
            mt5.shutdown()
        return

    try:
        # Create table, dropping if it exists (recreate=True)
        if not create_table(conn, cursor, recreate=True): # Set recreate=True to ensure fresh table with correct schema
            print("Failed to create or access table, exiting.")
            return

        # Fetch and store initial historical data
        print("Fetching initial historical data...")
        historical_data = fetch_initial_historical_data()
        if not historical_data.empty:
            insert_data(conn, cursor, historical_data)
            # No need for a separate print for count, insert_data handles it if rows > 0
        else:
            print("Failed to fetch initial historical data.")
            # Decide if to continue or exit if initial fetch fails
            print("Proceeding with real-time data collection despite no initial data...")


        # Main loop for continuous updates
        print("Starting continuous data collection...")
        while True:
            # Calculate seconds until the next fetch time (5 seconds after the day)
            seconds_to_next_fetch = calculate_seconds_to_next_fetch()

            print(f"Waiting {seconds_to_next_fetch} seconds until next fetch (5 seconds after the next day)...")
            time.sleep(seconds_to_next_fetch)

            # Fetch latest data (should be yesterday's completed candle)
            fetch_attempt_time = datetime.now() # Get current time before fetching
            latest_data = fetch_latest_data()

            if not latest_data.empty:
                # Store the completed candle to database
                rows = insert_data(conn, cursor, latest_data)
                if rows > 0:
                    print("\n--- Data updated at:", fetch_attempt_time, "---")
                    print("Latest stored candle (completed day):")
                    print(latest_data[['time', 'open', 'high', 'low', 'close', 'candle_type', 'range']])
                else:
                    print(f"\n--- Data fetched at: {fetch_attempt_time}, but it was a duplicate or insert failed. ---")

            else:
                print(f"\n--- No new data available or less than 2 candles fetched at: {fetch_attempt_time} ---")


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
            print("SQLite database connection closed")
        if mt5.terminal_state()[0]: # Check if MT5 is initialized
             mt5.shutdown()
             print("MetaTrader 5 connection shut down")
        else:
            print("MetaTrader 5 was not initialized or already shut down.")


if __name__ == "__main__":
    main()