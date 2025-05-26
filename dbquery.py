import psycopg2
import sys

def get_driver_names(vendor):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres", 
            user="postgres",
            port="5433",
            password="postgres"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT netmiko_driver_name, napalm_driver_name FROM network_drivers WHERE vendor = %s", (vendor,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        print(f"Database error: {e}")
        return None

if __name__ == "__main__":
    vendor = input("Enter vendor name: ")
    drivers = get_driver_names(vendor)
    if drivers:
        print(f"Netmiko driver: {drivers[0]}, NAPALM driver: {drivers[1]}")
    else:
        print("No drivers found for vendor or database error")
