from flask import Flask, request, jsonify, render_template
import requests
import os
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

app = Flask(__name__)

# ======================
# ENVIRONMENT VARIABLES
# ======================
TOKEN_ENDPOINT = os.getenv("TOKEN_ENDPOINT")
CORTEX_ENDPOINT = os.getenv("CORTEX_ENDPOINT")
STATEMENT_ENDPOINT = os.getenv("STATEMENT_ENDPOINT")
SEMANTIC_VIEW = os.getenv("SEMANTIC_VIEW")

OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID")
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

# Token cache
token_cache = {
    "access_token": ACCESS_TOKEN,
    "refresh_token": REFRESH_TOKEN
}


# ==========================================
# Refresh Token
# ==========================================
def refresh_access_token():
    global token_cache
    print("🔁 Refreshing access token...")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": token_cache["refresh_token"]
    }
    auth = (OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    resp = requests.post(TOKEN_ENDPOINT, data=data, auth=auth, headers=headers)
    if resp.status_code == 200:
        tokens = resp.json()
        token_cache["access_token"] = tokens.get("access_token")
        if "refresh_token" in tokens:
            token_cache["refresh_token"] = tokens["refresh_token"]
        print("✅ Access token refreshed successfully.")
    else:
        raise Exception(f"Token refresh failed: {resp.text}")


# ==========================================
# Step 1: Ask Cortex Analyst for SQL query
# ==========================================
def get_sql_from_cortex(prompt):
    headers = {
        "Authorization": f"Bearer {token_cache['access_token']}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "cortex-analyst",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_view": SEMANTIC_VIEW
    }

    # Log request payload
    print("\n===== Cortex Analyst Request =====")
    print(payload)
    print("==================================\n")

    response = requests.post(CORTEX_ENDPOINT, json=payload, headers=headers)

    if response.status_code == 401:
        refresh_access_token()
        headers["Authorization"] = f"Bearer {token_cache['access_token']}"
        response = requests.post(CORTEX_ENDPOINT, json=payload, headers=headers)

    print("\n===== Cortex Analyst Response =====")
    print("Status:", response.status_code)
    print(response.text)
    print("===================================\n")

    if response.status_code != 200:
        raise Exception(f"Cortex Analyst Error: {response.text}")

    res_json = response.json()
    print("Parsed JSON:")
    print(res_json)
    print("===================================\n")
    sql_query = None

    for msg in res_json.get("messages", []):
        print("Inspecting message:", msg)
        if msg.get("role") != "assistant":
            continue

        content = msg.get("content")
        print("Message content:", content)
        if isinstance(content, list):
            for item in content:
                print("Inspecting content item:", item)
                if item.get("type") == "text" and item.get("text"):
                    sql_query = item["text"].strip()
                    break
        elif isinstance(content, str) and content.strip():
            sql_query = content.strip()

        if sql_query:
            break

    if not sql_query:
        raise Exception(
            "Cortex Analyst did not return a SQL query. Check logs for details."
        )

    return sql_query


# ==========================================
# Step 2: Run SQL query on Snowflake
# ==========================================
def run_query_on_snowflake(sql_query):
    headers = {
        "Authorization": f"Bearer {token_cache['access_token']}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    payload = {
        "statement": sql_query,
        "timeout": 60,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "role": SNOWFLAKE_ROLE
    }

    print("\n===== Snowflake SQL Request =====")
    print("Payload:", payload)
    print("==================================\n")

    response = requests.post(STATEMENT_ENDPOINT, json=payload, headers=headers)

    if response.status_code == 401:
        refresh_access_token()
        headers["Authorization"] = f"Bearer {token_cache['access_token']}"
        response = requests.post(STATEMENT_ENDPOINT, json=payload, headers=headers)

    print("\n===== Snowflake SQL Response =====")
    print("Status:", response.status_code)
    print(response.text)
    print("==================================\n")

    if response.status_code != 200:
        raise Exception(f"Snowflake Query Execution Failed: {response.text}")

    return response.json()


# ==========================================
# Flask Routes
# ==========================================
@app.route('/')
def home():
    return render_template('index.html')


@app.route('/api/query', methods=['POST'])
def query():
    try:
        data = request.get_json(force=True)
        prompt = data.get("prompt", "").strip()
        if not prompt:
            return jsonify({"error": "Prompt is empty"}), 400

        # Step 1: Ask Cortex Analyst for SQL
        print("➡️ Received prompt:", prompt)
        sql_query = get_sql_from_cortex(prompt)
        print("✅ Generated SQL query:")
        print(sql_query)

        # Step 2: Run query on Snowflake
        result = run_query_on_snowflake(sql_query)
        print("✅ Snowflake query result:")
        print(result)

        return jsonify({
            "prompt": prompt,
            "sql_query": sql_query,
            "result": result
        })

    except Exception as e:
        print("❌ Exception:", str(e))
        return jsonify({"error": str(e)}), 500


@app.route('/favicon.ico')
def favicon():
    return '', 204


if __name__ == '__main__':
    app.run(port=9000, debug=True)
