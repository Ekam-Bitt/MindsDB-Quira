from flask import Flask, request, jsonify, send_from_directory
import requests, os, csv, uuid, time, psycopg2, traceback
from dotenv import load_dotenv
from config import PG_CONFIG

load_dotenv()  # Load environment variables from .env
openai_api_key = os.getenv("OPENAI_API_KEY")

app = Flask(__name__)

# MindsDB REST API URL
MINDSDB_URL = "http://127.0.0.1:47334/api"


# Check if database already exists in MindsDB
def database_exists(name):
    try:
        res = requests.get(f"{MINDSDB_URL}/databases")
        if res.ok:
            databases = (
                res.json()
                if isinstance(res.json(), list)
                else res.json().get("data", [])
            )
            return any(db.get("name") == name for db in databases)
    except Exception as e:
        print("Failed to check existing databases:", str(e))
    return False


# Register the PostgreSQL database with MindsDB
@app.before_request
def register_postgres_with_mindsdb():
    if database_exists("{PG_CONFIG['database']}"):
        print("Database '{PG_CONFIG['database']}' already registered with MindsDB.")
        return
    url = f"{MINDSDB_URL}/databases/"
    payload = {
        "database": {
            "name": PG_CONFIG["database"]+"_mindsdb",
            "engine": "postgres",
            "parameters": {
                "user": PG_CONFIG["user"],
                "password": PG_CONFIG["password"],
                "host": "host.docker.internal",
                "port": PG_CONFIG["port"],
                "database": PG_CONFIG["database"],
            },
        }
    }
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=payload, headers=headers)
        print("Database registration response:", response.status_code, response.text)
    except Exception as e:
        print("Failed to register PostgreSQL with MindsDB:", str(e))


def connect_postgres():
    return psycopg2.connect(**PG_CONFIG)


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


def run_query(query, retries=1, delay=1):
    for attempt in range(retries + 1):
        try:
            res = requests.post(f"{MINDSDB_URL}/sql/query", json={"query": query})
            res.raise_for_status()
            data = res.json()
            if data.get("type") == "error":
                raise RuntimeError(data.get("error_message") or "MindsDB error")
            return data
        except RuntimeError as e:
            if "Event loop is closed" in str(e) and attempt < retries:
                time.sleep(delay)
                continue
            raise
        except requests.RequestException as e:
            raise RuntimeError(f"Network or API error: {str(e)}")


@app.route("/upload", methods=["POST"])
def upload_csv():
    file = request.files.get("csvfile")
    if not file or not file.filename.endswith(".csv"):
        return jsonify({"error": "Invalid file. Please upload a CSV."}), 400

    table_name = "csv_" + uuid.uuid4().hex[:8]
    temp_path = f"/tmp/{table_name}.csv"
    file.save(temp_path)

    try:
        with open(temp_path, newline="") as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)
            columns = ", ".join([f'"{h}" TEXT' for h in headers])

        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE {table_name} (row_id SERIAL PRIMARY KEY, {columns});"
        )

        with open(temp_path, newline="") as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                placeholders = ", ".join(["%s"] * len(row))
                cur.execute(
                    f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders});",
                    row,
                )

        conn.commit()
        cur.close()
        conn.close()
        os.remove(temp_path)

    except Exception as e:
        return jsonify({"error": "Database error", "details": str(e)}), 500

    kb_name = "kb_" + table_name
    kb_query = f"""
    CREATE KNOWLEDGE BASE {kb_name}
    USING
        embedding_model = {{
        "provider": "ollama",
        "engine": "ollama_engine",
        "model_name": "nomic-embed-text",
        "base_url": "http://host.docker.internal:11434"
    }},
        reranking_model = {{
            "provider": "openai",
            "model_name": "gpt-4o",
            "api_key": "{openai_api_key}"
        }},
        metadata_columns = ['act'],
        content_columns = ['prompt'],
        id_column = 'row_id';
    """

    try:
        run_query(kb_query)
    except Exception as e:
        return (
            jsonify({"error": "Failed to create KB in MindsDB", "details": str(e)}),
            500,
        )

    return jsonify(
        {
            "kb_name": kb_name,
            "table_name": table_name,
            "headers": headers,
            "db_name": PG_CONFIG["database"],
        }
    )


@app.route("/insert", methods=["POST"])
def insert_into_kb():
    data = request.get_json()
    kb_name = data.get("kb")
    table_name = data.get("table")
    headers = data.get("headers")

    if not kb_name or not table_name or not headers:
        return jsonify({"error": "Missing kb, table, or headers"}), 400

    try:
        all_columns = ["row_id"] + headers
        column_list = ", ".join(all_columns)
        insert_query = f"""
        INSERT INTO {kb_name}
        SELECT {column_list}
        FROM {PG_CONFIG["database"]+"_mindsdb"}.{table_name};
        """
        run_query(insert_query)

    except Exception as e:
        print("INSERT ERROR:", traceback.format_exc())
        return (
            jsonify(
                {
                    "error": "Failed to insert into KB",
                    "details": str(e),
                    "query": insert_query,
                }
            ),
            500,
        )

    return jsonify({"status": "success", "inserted_into": kb_name})


@app.route("/ask", methods=["POST"])
def ask_question():
    data = request.get_json()
    kb = data.get("kb")
    question = data.get("question")

    if not kb or not question:
        return jsonify({"error": "Missing KB name or question"}), 400

    query = f"SELECT * FROM {kb} WHERE content = '{question}';"

    try:
        result = run_query(query)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": "Failed to query MindsDB", "details": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
