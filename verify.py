import sys
sys.path.insert(0, '.')
errors = []

try:
    from app.database import init_db, engine, jobs, job_results
    from sqlalchemy import inspect as sa_inspect
    tables = sa_inspect(engine).get_table_names()
    print("database OK — tables:", tables)
except Exception as e:
    errors.append("database: " + str(e))

try:
    from app.validator import validate_with_haiku, fetch_page_snippet, compute_haiku_cost
    cost = compute_haiku_cost(1700, 150)
    print("validator OK — sample call cost per 1700in/150out tokens:", round(cost, 6))
except Exception as e:
    errors.append("validator: " + str(e))

try:
    from app.job_runner import build_report, parse_gmaps_address, COLUMNS
    addr = parse_gmaps_address("906 Nuckolls St, Gadsden, AL 35903")
    print("job_runner OK — address parse:", addr)
    print("Output columns:", len(COLUMNS), "->", COLUMNS)
except Exception as e:
    errors.append("job_runner: " + str(e))

try:
    from app.main import app
    print("main OK")
except Exception as e:
    errors.append("main: " + str(e))

if errors:
    print()
    for e in errors:
        print("ERROR:", e)
else:
    print()
    print("All modules OK.")
