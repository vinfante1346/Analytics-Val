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
print("📊 Analyzing October 2025 logins by OS...\n")
ctx = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    private_key=pkb,
    role=ROLE,
    warehouse=WAREHOUSE
)

try:
    cursor = ctx.cursor()

    # Overall October statistics
    print("=" * 80)
    print("OCTOBER 2025 LOGIN STATISTICS")
    print("=" * 80 + "\n")

    overall_query = """
        SELECT
            COUNT(*) as total_sessions,
            COUNT(DISTINCT USER_ID) as unique_users,
            MIN(TIME) as first_login,
            MAX(TIME) as last_login
        FROM MYBAMBU_PROD.BAMBU_MART_HEAP.MART_SESSIONS
        WHERE TIME >= '2025-10-01'
        AND TIME < '2025-11-01'
    """

    cursor.execute(overall_query)
    result = cursor.fetchone()

    total_sessions = result[0]
    unique_users = result[1]
    first_login = result[2]
    last_login = result[3]

    print(f"📊 OVERALL OCTOBER STATISTICS:")
    print(f"   Total Sessions: {total_sessions:,}")
    print(f"   Unique Customers: {unique_users:,}")
    print(f"   First Login: {first_login}")
    print(f"   Last Login: {last_login}")
    print(f"   Avg Sessions per User: {total_sessions/unique_users:.1f}" if unique_users > 0 else "")

    # Breakdown by OS (extracting OS from platform)
    print("\n" + "=" * 80)
    print("BREAKDOWN BY OPERATING SYSTEM")
    print("=" * 80 + "\n")

    os_query = """
        SELECT
            CASE
                WHEN UPPER(PLATFORM) LIKE 'ANDROID%' THEN 'Android'
                WHEN UPPER(PLATFORM) LIKE 'IOS%' THEN 'iOS'
                WHEN UPPER(PLATFORM) LIKE 'WINDOWS%' THEN 'Windows'
                WHEN UPPER(PLATFORM) LIKE 'MAC OS%' THEN 'macOS'
                WHEN UPPER(PLATFORM) LIKE 'LINUX%' THEN 'Linux'
                WHEN UPPER(PLATFORM) LIKE 'CHROME OS%' THEN 'Chrome OS'
                ELSE 'Other'
            END as operating_system,
            COUNT(*) as total_sessions,
            COUNT(DISTINCT USER_ID) as unique_users,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_sessions,
            ROUND(COUNT(DISTINCT USER_ID) * 100.0 / SUM(COUNT(DISTINCT USER_ID)) OVER (), 2) as pct_of_users
        FROM MYBAMBU_PROD.BAMBU_MART_HEAP.MART_SESSIONS
        WHERE TIME >= '2025-10-01'
        AND TIME < '2025-11-01'
        GROUP BY operating_system
        ORDER BY unique_users DESC
    """

    cursor.execute(os_query)
    os_results = cursor.fetchall()

    print(f"{'OS':<15} {'Unique Users':<15} {'% of Users':<12} {'Sessions':<15} {'% Sessions':<12}")
    print("-" * 80)

    for row in os_results:
        os_name = row[0]
        sessions = row[1]
        users = row[2]
        pct_sessions = row[3]
        pct_users = row[4]

        print(f"{os_name:<15} {users:>14,} {pct_users:>11.1f}% {sessions:>14,} {pct_sessions:>11.1f}%")

    # Detailed platform breakdown (top 20)
    print("\n" + "=" * 80)
    print("TOP 20 PLATFORM VERSIONS")
    print("=" * 80 + "\n")

    platform_query = """
        SELECT
            PLATFORM,
            COUNT(*) as total_sessions,
            COUNT(DISTINCT USER_ID) as unique_users,
            ROUND(AVG(COUNT(*)) OVER (PARTITION BY SPLIT_PART(PLATFORM, ' ', 1)), 1) as avg_sessions_for_os
        FROM MYBAMBU_PROD.BAMBU_MART_HEAP.MART_SESSIONS
        WHERE TIME >= '2025-10-01'
        AND TIME < '2025-11-01'
        AND PLATFORM IS NOT NULL
        GROUP BY PLATFORM
        ORDER BY unique_users DESC
        LIMIT 20
    """

    cursor.execute(platform_query)
    platform_results = cursor.fetchall()

    print(f"{'Platform':<25} {'Unique Users':<15} {'Sessions':<15} {'Avg/User':<12}")
    print("-" * 80)

    for row in platform_results:
        platform = row[0]
        sessions = row[1]
        users = row[2]
        avg_sessions = sessions / users if users > 0 else 0

        print(f"{platform:<25} {users:>14,} {sessions:>14,} {avg_sessions:>11.1f}")

    # Daily breakdown for October
    print("\n" + "=" * 80)
    print("DAILY BREAKDOWN - OCTOBER 2025")
    print("=" * 80 + "\n")

    daily_query = """
        SELECT
            DATE(TIME) as login_date,
            COUNT(*) as daily_sessions,
            COUNT(DISTINCT USER_ID) as daily_unique_users
        FROM MYBAMBU_PROD.BAMBU_MART_HEAP.MART_SESSIONS
        WHERE TIME >= '2025-10-01'
        AND TIME < '2025-11-01'
        GROUP BY DATE(TIME)
        ORDER BY login_date
    """

    cursor.execute(daily_query)
    daily_results = cursor.fetchall()

    print(f"{'Date':<15} {'Sessions':<15} {'Unique Users':<15}")
    print("-" * 50)

    for row in daily_results:
        date = row[0]
        sessions = row[1]
        users = row[2]
        print(f"{date} {sessions:>14,} {users:>14,}")

    # Summary
    print("\n" + "=" * 80)
    print("✅ OCTOBER 2025 SUMMARY")
    print("=" * 80)
    print(f"\nTotal Unique Customers who logged in: {unique_users:,}")
    print(f"Total Sessions: {total_sessions:,}")
    print(f"Average: {total_sessions/unique_users:.1f} sessions per user" if unique_users > 0 else "")

finally:
    cursor.close()
    ctx.close()
