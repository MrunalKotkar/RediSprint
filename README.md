# 🚀 RediSprint — AI-Powered Sprint Planning

Automates sprint planning by reading your Google Sheet, enriching each ticket using semantic search over past tickets and your codebase (via Redis Vector Search), and creating fully-described Jira issues — all orchestrated by three AI agents communicating through Redis Streams.

> Built at the **AI Tinkerers SF — Agents with Superpowers Context Engineering Hackathon** (Nov 2024)

---

## Architecture

```
Google Sheet (sprint data)
        │
        ▼
┌─────────────────────┐    Redis Stream    ┌──────────────────────────┐    Redis Stream    ┌──────────────────┐
│  Agent 1 · Reader   │ ─────────────────► │  Agent 2 · Enricher      │ ─────────────────► │  Agent 3 · Creator│
│  (Composio Sheets)  │                    │  (Redis Vector Search)   │                    │  (Composio Jira) │
└─────────────────────┘                    └──────────────────────────┘                    └──────────────────┘
                                                       ▲
                                            tickets_idx + code_idx
                                            (seeded by ingest script)
```

---

## Prerequisites

You need accounts and API keys for these services:

| Service | Free tier | What for |
|---|---|---|
| [OpenAI](https://platform.openai.com) | Pay-per-use | GPT-4o + embeddings |
| [Redis Cloud](https://cloud.redis.io) | 30 MB free | Streams + Vector Search |
| [Composio](https://composio.dev) | Free | Google Sheets + Jira integrations |
| [Atlassian Jira](https://www.atlassian.com/software/jira) | Free (10 users) | Target for ticket creation |
| [Railway](https://railway.app) | $5 credit | Deployment |

---

## Local Setup

### 1. Clone & install

```bash
git clone https://github.com/YOUR_USERNAME/RediSprint.git
cd RediSprint
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

> **Note on `a2a-redis`:** This package was built for the Redis hackathon ecosystem. If it is not available on PyPI, install it from the Redis hackathon resources or contact the Redis team.

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in every value. See the table in `.env.example` for details.

### 3. Set up Composio

1. Sign up at [composio.dev](https://composio.dev)
2. In the dashboard, connect the **Google Sheets** integration
3. Connect the **Jira** integration (authorize your Atlassian account)
4. Copy your **API Key** and **User ID** into `.env`

### 4. Prepare your Google Sheet

1. Open `sample_data/google_sheet_template.csv` — this shows the exact format
2. Go to [sheets.google.com](https://sheets.google.com) → Create new sheet → File → Import → upload the CSV
3. Share the sheet: **Share → Anyone with the link → Viewer**
4. Copy the Sheet ID from the URL: `https://docs.google.com/spreadsheets/d/**SHEET_ID_HERE**/edit`
5. Add it to `.env` as `DEMO_GOOGLE_SHEET_ID`

**Required columns (Row 1 must be headers):**

| Column | Header | Example |
|---|---|---|
| A | Sprint | Sprint 3 |
| B | Type | Task / Bug / Story / Epic |
| C | Title | Add OAuth2 login |
| D | Client Impact | High / Medium / Low / Critical |
| E | Effort | 3, 5, 8, 13 (story points) |
| F | Owner | Jane Smith |
| G | Status | To Do / In Progress / Done |
| H | Assignee | Bob Johnson |

The system auto-detects the **latest sprint** (last unique value in column A) and only processes that sprint's tickets.

### 5. Seed Redis indices (one-time setup)

```bash
# Create the vector search indices
python create_indices.py

# Ingest past tickets + codebase for semantic context
python ingest_code_and_tickets.py
```

The ingestion script reads historical tickets from `sample_data/past_tickets.csv` by default. To use your own Jira export, set `TICKETS_CSV` in `.env`. To point at your own codebase for code-aware descriptions, set `CODE_DIR`.

### 6. Run locally

```bash
python web_ui.py
```

Open [http://localhost:8000](http://localhost:8000).

- The Sheet ID input is pre-filled with your `DEMO_GOOGLE_SHEET_ID`
- Any user can paste their own Sheet ID (same column format required)
- Click **Run Sprint Automation** and watch the three agents work in real time

---

## Deploy to Railway

### Option A: Railway (recommended)

1. Push this repo to GitHub
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub repo
3. Add a **Redis** plugin: Railway dashboard → + New → Database → Redis
4. Set all environment variables from `.env` in Railway's **Variables** tab
5. Railway auto-deploys from `Dockerfile` using `railway.toml`

Your app will be live at `https://your-project.up.railway.app`

### Option B: Render

1. Push to GitHub
2. [render.com](https://render.com) → New Web Service → connect repo
3. Build command: `pip install -r requirements.txt`
4. Start command: `python web_ui.py`
5. Add a **Redis** instance from Render's marketplace
6. Set env vars in Render's Environment settings

---

## Project Structure

```
RediSprint/
├── a2a_agents.py                # Three agents + Redis Streams orchestration
├── web_ui.py                    # Flask web interface (SSE streaming, sheet ID input)
├── ingest_code_and_tickets.py   # One-time data ingestion into Redis vector indices
├── create_indices.py            # Creates tickets_idx and code_idx in Redis
├── templates/
│   └── index.html               # Web UI (agent pipeline, sheet config, terminal)
├── sample_data/
│   ├── google_sheet_template.csv  # Import this into Google Sheets as your sprint input
│   └── past_tickets.csv           # Sample historical tickets for semantic context
├── .env.example                 # Environment variable template
├── requirements.txt
├── Dockerfile
├── railway.toml
└── .gitignore
```

---

## Google Sheet Format (detailed)

Download `sample_data/google_sheet_template.csv` and import it into Google Sheets to get started immediately. The sheet **must have Row 1 as headers** with these exact column names:

```
Sprint | Type | Title | Client Impact | Effort | Owner | Status | Assignee
```

The web UI also has a **"View required sheet format"** link that shows this table with examples inline.

---

## How It Works

1. **Agent 1 (Sprint Reader)** calls the Google Sheets API via Composio, reads all rows, finds the latest sprint name, and enqueues each ticket as a message on a Redis Stream.

2. **Agent 2 (Context Enricher)** dequeues each ticket, runs a hybrid vector+text search over `tickets_idx` (past Jira tickets) and `code_idx` (codebase), then calls GPT-4o to write an enriched description that references actual code files and past tickets. The enriched payload is pushed to the Jira creation stream.

3. **Agent 3 (Jira Creator)** dequeues each enriched ticket and uses the OpenAI Agents SDK with Composio's Jira tools to create a fully-populated Jira issue.

All inter-agent communication uses **Redis Streams**, making the pipeline async, observable, and easy to extend.

---

## License

MIT
