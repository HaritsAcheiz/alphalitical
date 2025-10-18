# Alphalitical – Airflow + Scraper
This project deploys Apache Airflow alongside a Gateway server container that hosts your Python project in ./news_scraper.
Airflow triggers your scripts on the Gateway via SSHOperator, and anything written under ./news_scraper/data is available locally thanks to a bind‑mount.

# Quick Start – Airflow + Gateway (news_scraper)

## 1. Generate SSH Keys
```bash
mkdir -p secrets/ssh
ssh-keygen -t ed25519 -f secrets/ssh/id_ed25519 -N ''
cp secrets/ssh/id_ed25519.pub secrets/ssh/authorized_keys
chmod 600 secrets/ssh/authorized_keys
```

---

## 2. Build & Start Services
```bash
docker compose up -d --build
```

---

## 3. Create Airflow SSH Connection
```bash
docker compose run --rm airflow-cli   airflow connections add ssh_gateway   --conn-type ssh   --conn-host gateway   --conn-login scraper   --conn-port 22   --conn-extra '{"key_file": "/opt/airflow/ssh/id_ed25519", "timeout": 120, "no_host_key_check": true}'
```

---

## 4. Add Your DAG
Place your DAG in `./dags/`. Example command in DAG:
```bash
cd /opt/news_scraper && source /opt/venvs/news_scraper/bin/activate && python scraper.py
```

---

## 5. Trigger the DAG
```bash
docker compose run --rm airflow-cli airflow dags trigger news_scraper_via_ssh
```

---

## 6. Check Logs
```bash
docker compose logs -f gateway
docker compose logs -f airflow-worker
```

✅ Done! Your `news_scraper` project runs on the Gateway via Airflow SSHOperator.
