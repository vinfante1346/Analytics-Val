#!/usr/bin/env python3
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

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
print("🔐 Analyzing MART_SESSIONS for login data...\n")
ctx = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    private_key=pkb,
    role=ROLE,
    warehouse=WAREHOUSE
)

try:
    cursor = ctx.cursor()

    # Get table structure
    print("=" * 80)
    print("MART_SESSIONS TABLE STRUCTURE")
    print("=" * 80 + "\n")

    cursor.execute("DESCRIBE TABLE MYBAMBU_PROD.BAMBU_MART_HEAP.MART_SESSIONS")
    columns = cursor.fetchall()

    print("Columns in MART_SESSIONS:")
    for col in columns:
        print(f"  • {col[0]} ({col[1]})")

    # Get session statistics for last 30 days
    print("\n" + "=" * 80)
    print("LOGIN/SESSION STATISTICS (LAST 30 DAYS)")
    print("=" * 80 + "\n")

    # Query for sessions in last 30 days
    query = """
        SELECT
            COUNT(*) as total_sessions,
            COUNT(DISTINCT USER_ID) as unique_users,
            MIN(TIME) as first_session,
            MAX(TIME) as last_session
        FROM MYBAMBU_PROD.BAMBU_MART_HEAP.MART_SESSIONS
        WHERE TIME >= DATEADD(day, -30, CURRENT_DATE())
    """

    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        total_sessions = result[0]
        unique_users = result[1]
        first_session = result[2]
        last_session = result[3]

        print(f"📊 OVERALL STATISTICS:")
        print(f"   Total Sessions: {total_sessions:,}")
        print(f"   Unique Users: {unique_users:,}")
        print(f"   First Session: {first_session}")
        print(f"   Last Session: {last_session}")
        print(f"   Avg Sessions per User: {total_sessions/unique_users:.1f}" if unique_users > 0 else "")

    # Daily breakdown
    print("\n📅 DAILY BREAKDOWN (Last 7 days):\n")

    daily_query = """
        SELECT
            DATE(TIME) as session_date,
            COUNT(*) as daily_sessions,
            COUNT(DISTINCT USER_ID) as daily_unique_users
        FROM MYBAMBU_PROD.BAMBU_MART_HEAP.MART_SESSIONS
        WHERE TIME >= DATEADD(day, -7, CURRENT_DATE())
        GROUP BY DATE(TIME)
        ORDER BY session_date DESC
    """

    cursor.execute(daily_query)
    daily_results = cursor.fetchall()

    for row in daily_results:
        date = row[0]
        sessions = row[1]
        users = row[2]
        print(f"   {date}: {sessions:,} sessions | {users:,} unique users")

    # Platform breakdown
    print("\n📱 PLATFORM BREAKDOWN (Last 30 days):\n")

    try:
        platform_query = """
            SELECT
                PLATFORM,
                COUNT(*) as sessions,
                COUNT(DISTINCT USER_ID) as unique_users
            FROM MYBAMBU_PROD.BAMBU_MART_HEAP.MART_SESSIONS
            WHERE TIME >= DATEADD(day, -30, CURRENT_DATE())
            GROUP BY PLATFORM
            ORDER BY sessions DESC
        """

        cursor.execute(platform_query)
        platform_results = cursor.fetchall()

        for row in platform_results:
            platform = row[0] or 'Unknown'
            sessions = row[1]
            users = row[2]
            print(f"   {platform}: {sessions:,} sessions | {users:,} users")

    except Exception as e:
        print(f"   (Platform data not available)")

    print("\n" + "=" * 80)
    print(f"✅ ANSWER: {total_sessions:,} logins/sessions in the last 30 days")
    print("=" * 80)

finally:
    cursor.close()
    ctx.close()
