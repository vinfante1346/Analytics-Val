#!/usr/bin/env python3
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from datetime import datetime, timedelta

# Connection parameters
ACCOUNT = 'GIJUXYU-ZV35737'
USER = 'VINFANTE'
ROLE = 'ACCOUNTADMIN'
WAREHOUSE = 'COMPUTE_WH'

# Private key
PRIVATE_KEY_PEM = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDWhYxhTfIuvPuY
U8jfZt2ilRmXKf9tLUWqU2Y8Wa1Mqkzi2WubsPXHyeQVo6qHaQJ09EtIDPMJWy86
g96/lXctCWU+vg+UahbpUo3d3/IGcAXaReVBI0pdjcHfxx8076LmvrArpy8uBouS
2Vq6T7XsD7NZkPHDSajT9QaYkGu974uw1G/Tjn0T1NEl4tHqdN6I51Lon9HKwR7g
h9EpvW25DK6E1u8xijMsPTjMaLTApQz/ESJVZHqLq+11OOsmOxVMIOeRCxqJqcuL
RiLoyOGSrTmG02HV/rgl99A77DVPlGSx7bsDEZ7Q5DNC9g2b7qnNGyijO1ZWzLZO
6rd9CXb5AgMBAAECggEAAR/Tfr4x2zuNw9G77juub2S5ObIDTerl+blOV6Z3AWgT
SnOuGer6XXXH8Kn18uSzt8+i903jzbu7sfOZjKbwI/sR6Kg5hS3csqVCIv0cGkXm
zFnh3P/APBPbI7APdro2VWhdNhwGqDF69I6G11sJ5QkNkF6o7GNAF/Rt67Q2eFkU
ap14Rxlbc28Gj9knotLfJ37XsUqGH33ScG5xbTKYzaH8ms0p16CJwGFwaqVTlQn/
b86prxWkjXEfPxmfOritda5y9CRte6yLeiY21znQ7tbx2iCcOqUbaa8nHSkvDZxg
7FSjapWm9IcLBcjLDS62BBLZOccnpz9Dd3pwgQfw4QKBgQDzevBiQjDOrgStx+pX
E2GbRlWwNfB7YB1GvP8wXdYf3Uy+c0oQoRapTfPoIl0pIRe1dCqxQ+b5OcIYT889
/q/gor4gvxlJiKrZoFX5LccnSD10VIuA7FeCRjiQqck1V+Z1JnsuPqqpFiS+4gE9
Nz3XmevMYf8XuzfIszfgSK3jUQKBgQDhjWmOEbsuCbxQ+XD4XZWuyEqd6lWkVtS9
AdJlHetaW4CohADUyHA5uLrge25wYhd9bNb+3VlBdW8KLtbtukoK96+kIfEzhQSy
BkwamITSAxPq56EWORaUAAteyflmv8UwUTsDs2uWoZhZ0CG3gX/3sBZhdvL1LmT1
Gsfu/UVfKQKBgQCMGi7ea3YIR6wbj1CyAE4G+kbuWWtiouDVxoUVALnopf+2C0MQ
JJGUpG14IuX2d7tbx1eVnxv2Rxz/vlTjOH1dxmefEjdrz7938MHn20agvPnXyZpo
eha0uNFttLU6A7Vxrc3tw1OSblKAoC3UWsg0GrbLaYxOzIUB8NZzMX8VsQKBgEjL
Gdj3EgDutW8wwev2UBujmqlSeqdaOrhxQRTPTijQRTqdt6L0uXt3iiBu1ZrBnbEm
ElEY4PiGTPrtWQJKUCEwBOik57Jn6LcH30HqHVumEKSMcum4LPhA92p1Jt+pXpuU
a8Zq/nsT1haOXINb8Q/gLajw+cJ1YbHVHdect+nZAoGAJM6iO+s01E9lPfH0bsPd
iH4VCDZ9FaEfXxXZZq6pJ5sqIBeWdbEWs5B0wGZsscAxIjwTIXDWJy/d9ovd75dg
U7plzwNMhkv+3J2XoFxw9hjaKG8H0VfhRZQ9Cjlry7u3el9FYP5a1VSYBh5tv1pz
MaePmeQKg4YKq9ohes7rkJI=
-----END PRIVATE KEY-----"""

# Parse private key
p_key = serialization.load_pem_private_key(
    PRIVATE_KEY_PEM.encode('utf-8'),
    password=None,
    backend=default_backend()
)

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Connect to Snowflake
print("🔐 Analyzing logins from MYBAMBU_PROD.BAMBU_MART_HEAP...\n")
ctx = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    private_key=pkb,
    role=ROLE,
    warehouse=WAREHOUSE
)

try:
    cursor = ctx.cursor()

    # First, list all tables in BAMBU_MART_HEAP
    print("=" * 80)
    print("TABLES IN BAMBU_MART_HEAP SCHEMA")
    print("=" * 80 + "\n")

    cursor.execute("SHOW TABLES IN SCHEMA MYBAMBU_PROD.BAMBU_MART_HEAP")
    tables = cursor.fetchall()
    table_columns = [desc[0] for desc in cursor.description]

    login_tables = []
    event_tables = []

    for table in tables:
        table_dict = dict(zip(table_columns, table))
        table_name = table_dict.get('name', '').upper()

        print(f"📋 {table_dict.get('name')}")

        if 'LOGIN' in table_name or 'SESSION' in table_name or 'AUTH' in table_name:
            login_tables.append(table_dict.get('name'))
            print(f"   ✅ Potential login table")
        elif 'EVENT' in table_name or 'TRACK' in table_name:
            event_tables.append(table_dict.get('name'))
            print(f"   📊 Event tracking table")

    # Calculate date 30 days ago
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    print(f"\n🗓️  Looking for logins since: {thirty_days_ago}")

    # Query login data
    print("\n" + "=" * 80)
    print("LOGIN STATISTICS (LAST 30 DAYS)")
    print("=" * 80 + "\n")

    # Try to find login events
    tables_to_check = login_tables + event_tables if login_tables or event_tables else []

    if not tables_to_check:
        # If no specific login tables, check all tables for login-related data
        cursor.execute("SHOW TABLES IN SCHEMA MYBAMBU_PROD.BAMBU_MART_HEAP")
        all_tables = cursor.fetchall()
        tables_to_check = [dict(zip(table_columns, t)).get('name') for t in all_tables]

    login_count = 0
    found_login_data = False

    for table_name in tables_to_check:
        try:
            full_table_name = f"MYBAMBU_PROD.BAMBU_MART_HEAP.{table_name}"

            # Get table structure
            cursor.execute(f"DESCRIBE TABLE {full_table_name}")
            columns = cursor.fetchall()
            column_names = [col[0].upper() for col in columns]

            # Look for login-related columns
            has_event_name = 'EVENT_NAME' in column_names or 'EVENT_TYPE' in column_names or 'ACTION' in column_names
            has_timestamp = any(col for col in column_names if 'TIME' in col or 'DATE' in col or 'CREATED' in col)

            if has_event_name and has_timestamp:
                # Find the timestamp column
                timestamp_col = next((col for col in column_names if 'TIME' in col or 'DATE' in col or 'CREATED' in col), None)
                event_col = next((col for col in column_names if col in ['EVENT_NAME', 'EVENT_TYPE', 'ACTION']), None)

                if timestamp_col and event_col:
                    # Query for login events
                    query = f"""
                        SELECT
                            COUNT(*) as login_count,
                            COUNT(DISTINCT CASE
                                WHEN {timestamp_col} >= DATEADD(day, -30, CURRENT_DATE())
                                THEN USER_ID
                            END) as unique_users
                        FROM {full_table_name}
                        WHERE {timestamp_col} >= DATEADD(day, -30, CURRENT_DATE())
                        AND (
                            UPPER({event_col}) LIKE '%LOGIN%'
                            OR UPPER({event_col}) LIKE '%SIGN%IN%'
                            OR UPPER({event_col}) LIKE '%AUTH%'
                        )
                    """

                    try:
                        cursor.execute(query)
                        result = cursor.fetchone()

                        if result and result[0] > 0:
                            found_login_data = True
                            login_count = result[0]
                            unique_users = result[1] if len(result) > 1 else 'N/A'

                            print(f"✅ Found in {table_name}:")
                            print(f"   Total Logins: {login_count:,}")
                            if unique_users != 'N/A':
                                print(f"   Unique Users: {unique_users:,}")

                            # Get daily breakdown
                            daily_query = f"""
                                SELECT
                                    DATE({timestamp_col}) as login_date,
                                    COUNT(*) as daily_logins,
                                    COUNT(DISTINCT USER_ID) as daily_unique_users
                                FROM {full_table_name}
                                WHERE {timestamp_col} >= DATEADD(day, -30, CURRENT_DATE())
                                AND (
                                    UPPER({event_col}) LIKE '%LOGIN%'
                                    OR UPPER({event_col}) LIKE '%SIGN%IN%'
                                    OR UPPER({event_col}) LIKE '%AUTH%'
                                )
                                GROUP BY DATE({timestamp_col})
                                ORDER BY login_date DESC
                                LIMIT 7
                            """

                            cursor.execute(daily_query)
                            daily_results = cursor.fetchall()

                            if daily_results:
                                print(f"\n   📅 Last 7 days breakdown:")
                                for row in daily_results:
                                    print(f"      {row[0]}: {row[1]:,} logins ({row[2]:,} unique users)")

                            print()

                    except Exception as e:
                        if "does not exist" not in str(e).lower():
                            pass  # Skip errors quietly

        except Exception as e:
            continue  # Skip to next table

    if not found_login_data:
        print("❌ No login data found in the last 30 days in BAMBU_MART_HEAP")
        print("\nLet me check for any user activity or session data...")

        # Try a more general query
        for table_name in tables_to_check[:5]:  # Check first 5 tables
            try:
                full_table_name = f"MYBAMBU_PROD.BAMBU_MART_HEAP.{table_name}"

                cursor.execute(f"DESCRIBE TABLE {full_table_name}")
                columns = cursor.fetchall()
                column_names = [col[0].upper() for col in columns]

                timestamp_col = next((col for col in column_names if 'TIME' in col or 'DATE' in col), None)

                if timestamp_col:
                    count_query = f"""
                        SELECT COUNT(*) as event_count
                        FROM {full_table_name}
                        WHERE {timestamp_col} >= DATEADD(day, -30, CURRENT_DATE())
                        LIMIT 1
                    """

                    cursor.execute(count_query)
                    result = cursor.fetchone()

                    if result and result[0] > 0:
                        print(f"\n📊 {table_name} has {result[0]:,} events in last 30 days")

            except:
                continue

    if found_login_data:
        print("\n" + "=" * 80)
        print(f"📊 SUMMARY: {login_count:,} total logins in the last 30 days")
        print("=" * 80)

finally:
    cursor.close()
    ctx.close()
