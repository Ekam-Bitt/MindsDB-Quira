import requests
import psycopg2
from config import PG_CONFIG

# MindsDB REST API URL
MINDSDB_URL = "http://127.0.0.1:47334/api"

# System DBs to retain
SYSTEM_DATABASES = {"information_schema", "log", "mindsdb", "files"}


def drop_all_mindsdb_kbs():
    """Drop all knowledge bases in MindsDB."""
    query = "SHOW KNOWLEDGE BASES;"
    res = requests.post(f"{MINDSDB_URL}/sql/query", json={"query": query})
    kbs = res.json().get("data", [])

    for row in kbs:
        kb_name = row[0]  # assuming name is in first column
        drop_sql = f"DROP KNOWLEDGE_BASE IF EXISTS {kb_name};"
        drop_res = requests.post(f"{MINDSDB_URL}/sql/query", json={"query": drop_sql})
        print(f"✅ Dropped KB: {kb_name} | Status: {drop_res.status_code}")


def drop_all_mindsdb_postgres_dbs():
    """Drop all MindsDB-registered PostgreSQL databases."""
    query = "SHOW DATABASES;"
    res = requests.post(f"{MINDSDB_URL}/sql/query", json={"query": query})
    dbs = res.json().get("data", [])

    for row in dbs:
        db_name = row[0] if isinstance(row, list) else row
        if db_name.startswith("postgres") and db_name not in SYSTEM_DATABASES:
            drop_sql = f"DROP DATABASE {db_name};"
            drop_res = requests.post(
                f"{MINDSDB_URL}/sql/query", json={"query": drop_sql}
            )
            print(f"✅ Dropped DB: {db_name} | Status: {drop_res.status_code}")


def drop_all_postgres_tables():
    """Drop all user-created PostgreSQL tables."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
        tables = cur.fetchall()

        for table in tables:
            tname = table[0]
            cur.execute(f'DROP TABLE IF EXISTS "{tname}" CASCADE;')
            print(f"🗑️ Dropped table: {tname}")

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print("❌ Error dropping tables:", e)


if __name__ == "__main__":
    print("\n⚠️  Starting cleanup of KBs, MindsDB-Postgres DBs and PG tables...\n")
    drop_all_mindsdb_kbs()
    drop_all_mindsdb_postgres_dbs()
    drop_all_postgres_tables()
    print("\n✅ Cleanup complete.")
