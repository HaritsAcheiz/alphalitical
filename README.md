# Alphalitical – Airflow + Scraper
This project deploys Apache Airflow alongside a Gateway server container that hosts your Python project in ./news_scraper.
Airflow triggers your scripts on the Gateway via SSHOperator, and anything written under ./news_scraper/data is available locally thanks to a bind‑mount.

