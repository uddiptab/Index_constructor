A Dockerized backend system using FastAPI, DuckDB, and Redis that dynamically tracks and manages a custom equal-weighted stock index composed of the Top 100 US stocks by market cap.

🚀 Features
Build equal-weighted index from historical market data

Retrieve index performance and composition

Detect and show composition changes

Export all data into clean Excel files

Redis caching for speed and efficiency

SQL-only analytics using DuckDB

Containerized for local development

🧱 Tech Stack
Python 3.11

FastAPI

DuckDB (storage)

Redis (caching)

Pandas

yFinance (data source)

Docker + Docker Compose

⚙️ Getting Started
🔁 1. Clone the Repo
bash
Copy
Edit
git clone https://github.com/your-username/equal-weight-index-project.git
cd equal-weight-index-project
🐳 2. Run via Docker
bash
Copy
Edit
docker-compose up --build
🔗 3. Visit API Docs
Once up, go to:
http://localhost:8000/docs
(Interactive Swagger UI)
