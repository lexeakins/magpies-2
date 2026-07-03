import sqlite3, json

conn = sqlite3.connect(r"C:\Users\Alex Eakins\Desktop\Accounts_Receivable_Contacts\magpie_app\data\magpie.db")
cur = conn.cursor()

# Get the Perplexity calls from TEST 2
cur.execute("""
    SELECT company, stage, input_tokens, output_tokens, cost_usd, stop_reason, raw_response, latency_ms
    FROM api_calls
    WHERE job_id = '211518b6' AND provider = 'perplexity'
    LIMIT 5
""")
rows = cur.fetchall()

print("PERPLEXITY API CALL DETAILS (TEST 2):")
for r in rows:
    print(f"\n  Company:      {r[0]}")
    print(f"  Stage:        {r[1]}")
    print(f"  Tokens in/out:{r[2]}/{r[3]}")
    print(f"  Cost:         ${r[4]}")
    print(f"  Stop reason:  {r[5]}")
    print(f"  Latency:      {r[7]}ms")
    print(f"  Raw response: {r[6]}")

conn.close()
