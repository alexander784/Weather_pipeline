import psycopg2
from datetime import datetime
from api_request import fetch_data


def connect_to_db():
    print("Connecting to the PostgreSQL database...")

    try:
        conn = psycopg2.connect(
            host="postgres_container",
            port="5432",
            dbname="db",
            user="db_user",
            password="db_password"
        )
        print("Database connection successful.")
        return conn

    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise


def create_table(conn):
    print("Creating schema and table if they do not exist...")

    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;

            CREATE TABLE IF NOT EXISTS dev.raw_weather_data (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature FLOAT,
                weather_descriptions TEXT,
                wind_speed FLOAT,
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT NOW(),
                utc_offset TEXT
            );
        """)
        conn.commit()
        cursor.close()
        print("Schema and table created successfully.")

    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        conn.rollback()
        raise


def insert_records(conn, data):
    print("Inserting data into the database...")

    insert_query = """
        INSERT INTO dev.raw_weather_data (
            city,
            temperature,
            weather_descriptions,
            wind_speed,
            time,
            utc_offset
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    try:
        cursor = conn.cursor()
        cursor.execute(
            insert_query,
            (
                data["city"],
                data["temperature"],
                data["weather_descriptions"],
                data["wind_speed"],
                data["time"],
                data["utc_offset"]
            )
        )
        conn.commit()
        cursor.close()
        print("Data inserted successfully.")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Failed to insert data: {e}")
        raise


def main():
    conn = connect_to_db()
    create_table(conn)

    api_response = fetch_data()
    
    if not api_response:
        raise ValueError("API response is empty")
    
    if "error" in api_response:
        raise ValueError(f"API Error: {api_response['error']}")
    
    if "location" not in api_response or "current" not in api_response:
        raise ValueError(f"Unexpected API response structure: {api_response}")

    data = {
        "city": api_response["location"]["name"],
        "temperature": api_response["current"]["temperature"],
        "weather_descriptions": ", ".join(api_response["current"]["weather_descriptions"]),
        "wind_speed": api_response["current"]["wind_speed"],
        "time": datetime.utcnow(),
        "utc_offset": api_response["location"]["utc_offset"]
    }

    insert_records(conn, data)
    conn.close()


if __name__ == "__main__":
    main()