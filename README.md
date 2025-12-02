FastAPI + React Lakehouse App (Databricks)
=========================================

This is a minimal template suitable for packaging as a Databricks Lakehouse App.

Structure:
- app/: FastAPI backend that serves the frontend build and exposes /api/finance-data
- frontend/build/: prebuilt static assets (placeholder). Replace with `npm run build` output.
- requirements.txt: Python dependencies
- databricks.yml: App manifest for Lakehouse Apps

How to use:
1. Replace frontend/build with your real React `npm run build` directory.
2. Update the SQL query in app/query_service.py to point to your Unity Catalog or database table.
3. Zip this folder and upload it as a Lakehouse App in Databricks (or use repo -> Apps).
4. Deploy; Databricks will run the entry_point program and serve the app.

Notes:
- In Lakehouse App runtime, Databricks injects credentials so you don't need an access token.
- For local testing you may need to set DATABRICKS_SERVER_HOSTNAME and DATABRICKS_SQL_HTTP_PATH env vars.