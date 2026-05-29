"""
A2A (Agent-to-Agent) Sprint Planning System
Three agents collaborate via Redis Streams:
1. SprintReaderAgent: Reads Google Sheets sprint data
2. ContextEnricherAgent: Searches tickets/code and enriches descriptions
3. JiraCreatorAgent: Creates Jira tickets
"""
import os, asyncio, textwrap, json, copy
from dotenv import load_dotenv
from composio import Composio
from composio_openai_agents import OpenAIAgentsProvider
from agents import Agent, Runner, set_default_openai_client, set_default_openai_api
from openai import OpenAI, AsyncOpenAI
from redisvl.index import SearchIndex
from redisvl.query import VectorQuery
from redisvl.utils.vectorize import HFTextVectorizer
import redis.asyncio as aioredis


class _StreamQueue:
    """Minimal Redis Streams queue replacing a2a_redis."""
    def __init__(self, client, stream_name: str):
        self._client = client
        self._stream = stream_name
        self._last_id = "0"

    async def enqueue_event(self, event: dict):
        await self._client.xadd(self._stream, {"data": json.dumps(event)})

    async def dequeue_event(self, no_wait: bool = False):
        result = await self._client.xread({self._stream: self._last_id}, count=1)
        if not result:
            raise RuntimeError("Queue empty")
        _, messages = result[0]
        msg_id, fields = messages[0]
        self._last_id = msg_id
        return json.loads(fields["data"])

    def task_done(self):
        pass


class _QueueManager:
    def __init__(self, client, prefix: str):
        self._client = client
        self._prefix = prefix

    async def create_or_tap(self, name: str) -> _StreamQueue:
        full_name = f"{self._prefix}{name}"
        await self._client.delete(full_name)
        return _StreamQueue(self._client, full_name)


def _clean_tools_for_groq(tools):
    """Remove strict=None fields that Groq rejects but OpenAI tolerates."""
    result = []
    for tool in tools:
        try:
            if hasattr(tool, "model_dump"):
                t = tool.model_dump()
            elif isinstance(tool, dict):
                t = copy.deepcopy(tool)
            else:
                t = copy.deepcopy(dict(tool))
            fn = t.get("function")
            if isinstance(fn, dict):
                fn.pop("strict", None)
            result.append(t)
        except Exception:
            result.append(tool)
    return result


load_dotenv()

# ---- Configuration ----
REDIS_URL = os.environ["REDIS_URL"]
QUEUE_PREFIX = "a2a:sprint:"
QUEUE_ENRICH = "enrich-tickets"
QUEUE_CREATE = "create-jira"

# Groq is OpenAI-API-compatible and has a generous free tier
GROQ_API_KEY = os.environ["GROQ_API_KEY"]
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
OPENAI_COMPLETION_MODEL = os.environ.get("OPENAI_COMPLETION_MODEL", "llama-3.3-70b-versatile")
HF_EMBEDDING_MODEL = os.environ.get("HF_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

# File upload path takes priority over Google Sheet ID
UPLOAD_FILE_PATH = os.environ.get("UPLOAD_FILE_PATH", "")
SPREADSHEET_ID = os.environ.get("GOOGLE_SHEET_ID") or os.environ.get("DEMO_GOOGLE_SHEET_ID", "")
if not UPLOAD_FILE_PATH and not SPREADSHEET_ID:
    raise ValueError("Either UPLOAD_FILE_PATH or GOOGLE_SHEET_ID must be set in environment")

SHEET_RANGE = os.environ.get("GOOGLE_SHEET_RANGE", "Sheet1!A1:J1000")

COMPOSIO_USER_ID = os.environ["COMPOSIO_USER_ID"]
COMPOSIO_API_KEY = os.environ.get("COMPOSIO_API_KEY")

JIRA_PROJECT_KEY = os.environ.get("JIRA_PROJECT_KEY", "SPRINT")
JIRA_DEFAULT_COMPONENT = os.environ.get("JIRA_COMPONENT_DEFAULT", "")

# Point openai-agents SDK at Groq using chat completions (Groq has no Responses API)
groq_async_client = AsyncOpenAI(api_key=GROQ_API_KEY, base_url=GROQ_BASE_URL)
set_default_openai_client(groq_async_client)
set_default_openai_api("chat_completions")

# Two separate Composio clients:
# 1. For Agent/Runner pattern (Jira agent) - with OpenAIAgentsProvider
composio_agents = Composio(api_key=COMPOSIO_API_KEY, provider=OpenAIAgentsProvider()) if COMPOSIO_API_KEY else Composio(provider=OpenAIAgentsProvider())
# 2. For direct API calls (Sheet reader)
composio_direct = Composio(api_key=COMPOSIO_API_KEY) if COMPOSIO_API_KEY else Composio()

# Groq-backed OpenAI client for direct completions
openai_client = OpenAI(api_key=GROQ_API_KEY, base_url=GROQ_BASE_URL)

tickets_idx = SearchIndex.from_existing("tickets_idx", redis_url=REDIS_URL)
code_idx = SearchIndex.from_existing("code_idx", redis_url=REDIS_URL)
# Local HuggingFace embeddings — free, no API key needed
embedder = HFTextVectorizer(model=HF_EMBEDDING_MODEL)

# Sheet columns: Sprint | Type | Title | Client Impact | Effort | Owner | Status | Assignee
COLS = ["Sprint", "Type", "Title", "Client Impact", "Effort", "Owner", "Status", "Assignee"]

PRIORITY_MAP = {
    "High": 1,
    "Medium": 2,
    "Low": 3,
    "Critical": 0
}


def read_sprint_from_file(file_path: str):
    """Read all rows (including header) from a CSV or Excel file."""
    import csv as _csv
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        with open(file_path, newline="", encoding="utf-8-sig") as f:
            return list(_csv.reader(f))
    elif ext in (".xlsx", ".xls"):
        import openpyxl
        wb = openpyxl.load_workbook(file_path, data_only=True)
        ws = wb.active
        return [[str(cell.value) if cell.value is not None else "" for cell in row]
                for row in ws.iter_rows()]
    else:
        raise ValueError(f"Unsupported file type '{ext}'. Use .csv, .xlsx, or .xls")


def get_priority_from_impact(client_impact: str) -> int:
    for key in PRIORITY_MAP:
        if key.lower() in client_impact.lower():
            return PRIORITY_MAP[key]
    return 2


def find_latest_sprint(rows):
    sprints = []
    for row in rows:
        if len(row) > 0 and row[0]:
            sprint_name = row[0].strip()
            if sprint_name and sprint_name not in sprints:
                sprints.append(sprint_name)
    return sprints[-1] if sprints else None


def search_relevant_context(query_text, component=None, k_tickets=5, k_code=3):
    qemb = embedder.embed(query_text)
    if hasattr(qemb, "tolist"):
        qemb = qemb.tolist()

    from redisvl.query.filter import Tag
    comp_filter = (Tag("component") == component) if component else None

    tq = VectorQuery(
        vector=qemb,
        vector_field_name="embedding",
        num_results=k_tickets,
        return_fields=["id", "title", "assignee", "issuetype", "component", "body"],
        filter_expression=comp_filter,
    )
    tickets_hits = tickets_idx.query(tq)

    cq = VectorQuery(
        vector=qemb,
        vector_field_name="embedding",
        num_results=k_code,
        return_fields=["id", "repo", "path", "lang", "body"]
    )
    code_hits = code_idx.query(cq)

    return tickets_hits, code_hits


def generate_enriched_description(row_data, tickets_hits, code_hits):
    title = row_data.get("title", "")
    itype = row_data.get("type", "Task")
    impact = row_data.get("client_impact", "")
    effort = row_data.get("effort", "")
    sprint = row_data.get("sprint", "")

    refs_tickets = "\n\n".join([
        f"**{h['id']}: {h['title']}**\n{h.get('body', 'No description')[:500]}..."
        for h in tickets_hits[:3]
    ]) or "No relevant past tickets found"

    refs_code = "\n\n".join([
        f"File: {h['path']} ({h['lang']})\n```{h['lang']}\n{h.get('body', '')[:500]}\n```"
        for h in code_hits[:3]
    ]) or "No relevant code files found"

    prompt = f"""
You are writing a Jira ticket description for: "{title}"

Context:
- Type: {itype}
- Priority: {impact}
- Story Points: {effort}
- Sprint: {sprint}

Similar past tickets for reference:
{refs_tickets}

CODE ANALYSIS - Relevant codebase files found:
{refs_code}

Write a clear, concise Jira ticket description in PLAIN TEXT (no markdown headers). Keep it under 350 words.

IMPORTANT: You MUST analyze the actual code shown above and reference specific:
- Function names, class names, or methods that need modification
- File paths where changes will be made
- Current implementation details visible in the code snippets

Structure your description as:

1. OVERVIEW (2-3 sentences): What needs to be done and why

2. CODE ANALYSIS: Based on the codebase snippets above, identify which specific functions/classes/files need changes. Reference actual function names and explain what modifications are needed. This section should prove you've analyzed the real code.

3. WHY IT MATTERS: Justify the {impact} priority with concrete impact on users or system

4. ACCEPTANCE CRITERIA (simple bullet list with dashes):
- Specific, testable outcomes
- Include at least one criterion about the code changes

5. TESTING: Brief note on how to verify the changes work

Write in clear paragraphs. Be specific about code - this proves we've done semantic search on the codebase!
"""

    response = openai_client.chat.completions.create(
        model=OPENAI_COMPLETION_MODEL,
        messages=[{"role": "user", "content": textwrap.dedent(prompt)}]
    )

    return response.choices[0].message.content


# ===== AGENT 1: Sprint Reader =====
async def sprint_reader_agent(enrich_queue):
    print("\n[AGENT 1: Sprint Reader] Starting...")

    if UPLOAD_FILE_PATH:
        # ---- Read from uploaded CSV / Excel file ----
        print(f"[AGENT 1] 📂 Reading from file: {os.path.basename(UPLOAD_FILE_PATH)}")
        try:
            all_rows = read_sprint_from_file(UPLOAD_FILE_PATH)
        except Exception as e:
            print(f"[AGENT 1] ❌ Failed to read file: {e}")
            return
        if not all_rows or len(all_rows) < 2:
            print("[AGENT 1] ❌ File is empty or has no data rows")
            return
        header = [h.strip() for h in all_rows[0]]
        rows = all_rows[1:]
        print(f"[AGENT 1] 📊 Found {len(rows)} rows with columns: {header}")
    else:
        # ---- Read from Google Sheets via Composio ----
        all_tools = composio_direct.tools.get(user_id=COMPOSIO_USER_ID, toolkits=["GOOGLESHEETS"])
        slugs = []
        for t in all_tools:
            if isinstance(t, dict):
                name = t.get("function", {}).get("name", "")
            else:
                name = getattr(getattr(t, "function", None), "name", "")
            if name:
                slugs.append(name)
        print(f"[AGENT 1] Available Google Sheets tools: {slugs}")

        read_slug = (
            next((s for s in slugs if "BATCH_GET" in s.upper()), None)
            or next((s for s in slugs if "GET" in s.upper() and "VALUE" in s.upper()), None)
            or next((s for s in slugs if "GET" in s.upper() and "RANGE" in s.upper()), None)
        )
        if not read_slug:
            print(f"[AGENT 1] ERROR: No value-reading tool found in: {slugs}")
            return
        print(f"[AGENT 1] Using slug: {read_slug}")

        values = None
        for args in [
            {"spreadsheetId": SPREADSHEET_ID, "ranges": [SHEET_RANGE]},
            {"spreadsheetId": SPREADSHEET_ID, "ranges": SHEET_RANGE},
        ]:
            result = composio_direct.tools.execute(
                slug=read_slug,
                arguments=args,
                user_id=COMPOSIO_USER_ID,
                dangerously_skip_version_check=True,
            )
            print(f"[AGENT 1] Tool result (successful={result.get('successful')}): {str(result)[:300]}")
            if result.get("successful"):
                data = result.get("data", {})
                value_ranges = data.get("valueRanges", [])
                if value_ranges:
                    values = value_ranges[0].get("values", [])
                    break
                if isinstance(data.get("values"), list):
                    values = data["values"]
                    break

        if not values or len(values) < 2:
            print("[AGENT 1] ❌ No data found in sheet")
            return

        header = [h.strip() for h in values[0]]
        rows = values[1:]
        print(f"[AGENT 1] 📊 Found {len(rows)} rows with columns: {header}")

    latest_sprint = find_latest_sprint(rows)
    print(f"[AGENT 1] 🎯 Latest sprint identified: {latest_sprint}")

    def parse_row(row_values):
        row_dict = {header[i]: row_values[i] if i < len(row_values) else ""
                    for i in range(len(header))}
        return {
            "sprint": row_dict.get(COLS[0], ""),
            "type": row_dict.get(COLS[1], "Task"),
            "title": row_dict.get(COLS[2], ""),
            "client_impact": row_dict.get(COLS[3], ""),
            "effort": row_dict.get(COLS[4], ""),
            "owner": row_dict.get(COLS[5], ""),
            "status": row_dict.get(COLS[6], ""),
            "assignee": row_dict.get(COLS[7], ""),
        }

    count = 0
    for row in rows:
        parsed = parse_row(row)
        if not parsed["title"].strip():
            continue
        if latest_sprint and parsed["sprint"] != latest_sprint:
            continue

        priority = get_priority_from_impact(parsed["client_impact"])
        parsed["priority"] = priority

        await enrich_queue.enqueue_event({"type": "EnrichTicket", "data": parsed})
        count += 1
        print(f"[AGENT 1] ✅ Enqueued: {parsed['title'][:60]}... (Priority: {priority})")
        await asyncio.sleep(0.1)

    print(f"\n[AGENT 1] 🎉 Finished! Sent {count} tickets for enrichment\n")


# ===== AGENT 2: Context Enricher =====
async def context_enricher_agent(enrich_queue, create_queue):
    print("\n🔎 [AGENT 2: Context Enricher] Starting...")

    processed = 0
    empty_polls = 0
    max_empty_polls = 2

    while True:
        event = None
        try:
            try:
                event = await enrich_queue.dequeue_event(no_wait=True)
                empty_polls = 0
            except RuntimeError:
                await asyncio.sleep(2)
                event = None
                empty_polls += 1
                if empty_polls >= max_empty_polls:
                    if processed > 0:
                        print(f"\n[AGENT 2] Finished enriching {processed} tickets\n")
                    else:
                        print(f"\n[AGENT 2] Queue empty — no tickets to enrich\n")
                    break
                continue
        except Exception as e:
            print(f"[AGENT 2] Dequeue error: {e}")
            continue

        if not event:
            continue

        try:
            if event.get("type") == "EnrichTicket":
                data = event["data"]
                print(f"\n[AGENT 2] 🔍 Enriching: {data['title'][:60]}...")

                query = f"{data['title']}\n{data['client_impact']}\n{data['effort']}"
                tickets_hits, code_hits = search_relevant_context(
                    query,
                    component=JIRA_DEFAULT_COMPONENT or None,
                    k_tickets=5,
                    k_code=3
                )

                print(f"[AGENT 2]   📋 Found {len(tickets_hits)} relevant tickets")
                print(f"[AGENT 2]   💻 Found {len(code_hits)} relevant code files")

                enriched_desc = generate_enriched_description(data, tickets_hits, code_hits)

                jira_payload = {
                    "row_data": data,
                    "description": enriched_desc,
                    "tickets_context": [{"id": h["id"], "title": h["title"]} for h in tickets_hits[:3]],
                    "code_context": [{"path": h["path"], "lang": h["lang"]} for h in code_hits[:3]]
                }

                await create_queue.enqueue_event({"type": "CreateJiraTicket", "payload": jira_payload})
                print(f"[AGENT 2] 📤 Enqueued for Jira creation")
                await asyncio.sleep(0.1)
                processed += 1
                print(f"[AGENT 2] ✅ Enriched and forwarded to Jira creator")
            else:
                print(f"[AGENT 2] ⚠️  Unknown event type: {event.get('type')}")
        except Exception as e:
            print(f"[AGENT 2] ❌ Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            enrich_queue.task_done()


# ===== AGENT 3: Jira Creator =====
async def jira_creator_agent(create_queue):
    print("\n[AGENT 3: Jira Creator] Starting...")

    # ── Tool discovery ──────────────────────────────────────────────────────
    # Composio returns at most 20 tools per call when using toolkits=[].
    # Fetch the first batch, then explicitly request the additional slugs we need
    # so we're not blocked by the default page limit.
    def _extract_slugs(tool_list):
        names = []
        for t in tool_list:
            n = t.get("function", {}).get("name", "") if isinstance(t, dict) else getattr(getattr(t, "function", None), "name", "")
            if n:
                names.append(n)
        return names

    # Fetch all Jira tools — try with a high limit first, fall back to paginating
    slugs = []
    try:
        all_tools = composio_direct.tools.get(user_id=COMPOSIO_USER_ID, toolkits=["JIRA"], limit=200)
        slugs = _extract_slugs(all_tools)
    except TypeError:
        # limit= not supported — paginate manually in steps of 20
        offset = 0
        while True:
            try:
                batch = composio_direct.tools.get(user_id=COMPOSIO_USER_ID, toolkits=["JIRA"], offset=offset)
            except TypeError:
                batch = composio_direct.tools.get(user_id=COMPOSIO_USER_ID, toolkits=["JIRA"])
                slugs = _extract_slugs(batch)
                break
            names = _extract_slugs(batch)
            if not names:
                break
            slugs.extend(n for n in names if n not in slugs)
            if len(names) < 20:
                break
            offset += 20

    print(f"[AGENT 3] Available Jira tools ({len(slugs)}): {slugs}")

    # Exact-match helpers — always prefer specifics over partials
    def exact(slug):
        return slug if slug in slugs else None

    def find_slug(*keywords, exclude=None):
        kw = [k.upper() for k in keywords]
        exc = [e.upper() for e in (exclude or [])]
        for s in slugs:
            su = s.upper()
            if all(k in su for k in kw) and not any(e in su for e in exc):
                return s
        return None

    # Use exact names where known; fall back to pattern search
    create_slug      = exact("JIRA_CREATE_ISSUE")           or find_slug("CREATE", "ISSUE", exclude=["BULK"])
    assign_slug      = exact("JIRA_ASSIGN_ISSUE")
    transition_slug  = exact("JIRA_TRANSITION_ISSUE")       or find_slug("TRANSITION", "ISSUE")
    get_trans_slug   = exact("JIRA_GET_TRANSITIONS") or exact("JIRA_GET_ISSUE_TRANSITIONS") or find_slug("GET", "TRANSITION")
    boards_slug      = exact("JIRA_LIST_BOARDS") or exact("JIRA_GET_ALL_BOARDS") or find_slug("LIST", "BOARD") or find_slug("GET", "BOARD", exclude=["SPRINT"])
    sprints_slug     = exact("JIRA_LIST_SPRINTS") or exact("JIRA_GET_AGILE_BOARD_SPRINTS") or exact("JIRA_GET_BOARD_SPRINTS") or find_slug("LIST", "SPRINT") or find_slug("SPRINT", "BOARD")
    move_sprint_slug = exact("JIRA_MOVE_ISSUE_TO_SPRINT") or exact("JIRA_MOVE_ISSUES_TO_SPRINT") or find_slug("MOVE", "SPRINT")
    create_sprint_slug = exact("JIRA_CREATE_SPRINT")
    update_sprint_slug = exact("JIRA_UPDATE_SPRINT")        or find_slug("UPDATE", "SPRINT")
    edit_issue_slug  = exact("JIRA_EDIT_ISSUE") or exact("JIRA_UPDATE_ISSUE") or find_slug("EDIT", "ISSUE") or find_slug("UPDATE", "ISSUE", exclude=["SPRINT", "BULK"])

    print(f"[AGENT 3] create={create_slug} | assign={assign_slug} | transition={transition_slug} | get_trans={get_trans_slug}")
    print(f"[AGENT 3] boards={boards_slug} | sprints={sprints_slug} | move_sprint={move_sprint_slug}")
    print(f"[AGENT 3] create_sprint={create_sprint_slug} | update_sprint={update_sprint_slug}")

    if not create_slug:
        print("[AGENT 3] ERROR: No create-issue tool found — aborting")
        return

    # ── Priority / Status maps ───────────────────────────────────────────────
    PRIORITY_NAMES = {0: "Highest", 1: "High", 2: "Medium", 3: "Low", 4: "Lowest"}
    STATUS_MAP = {
        "to do": "To Do", "todo": "To Do",
        "in progress": "In Progress", "inprogress": "In Progress", "in-progress": "In Progress",
        "done": "Done", "completed": "Done", "complete": "Done",
    }

    # ── Sprint pre-fetch + auto-create ──────────────────────────────────────
    # Build {sprint_name: sprint_id}. If no active sprint exists, create and
    # start one so tickets land on the board (not buried in the backlog).
    sprint_map: dict = {}       # name → id
    sprint_state_map: dict = {} # name → state ("active"/"future"/"closed")
    active_sprint_id = None
    board_id = None

    if boards_slug:
        try:
            # Try multiple param names — Composio tools vary between camelCase and snake_case
            br = None
            for board_args in [
                {"projectKeyOrId": JIRA_PROJECT_KEY},
                {"project_key": JIRA_PROJECT_KEY},
                {},
            ]:
                _r = composio_direct.tools.execute(
                    slug=boards_slug,
                    arguments=board_args,
                    user_id=COMPOSIO_USER_ID,
                    dangerously_skip_version_check=True,
                )
                print(f"[AGENT 3] Board fetch ({board_args}): successful={_r.get('successful')} data_keys={list((_r.get('data') or {}).keys())}")
                if _r.get("successful"):
                    br = _r
                    break

            if br:
                board_data = br.get("data", {})
                boards = (board_data.get("values") or board_data.get("boards")
                          or board_data.get("results") or [])
                print(f"[AGENT 3] Boards found: {len(boards)}")
                if boards:
                    board_id = boards[0].get("id")
                    print(f"[AGENT 3] Board ID: {board_id} (type={boards[0].get('type')})")

                    if sprints_slug and board_id:
                        # Try both camelCase and snake_case board ID param
                        sr = None
                        for sprint_args in [
                            {"boardId": board_id},
                            {"board_id": board_id},
                        ]:
                            _sr = composio_direct.tools.execute(
                                slug=sprints_slug,
                                arguments=sprint_args,
                                user_id=COMPOSIO_USER_ID,
                                dangerously_skip_version_check=True,
                            )
                            print(f"[AGENT 3] Sprint fetch ({sprint_args}): successful={_sr.get('successful')} data_keys={list((_sr.get('data') or {}).keys())}")
                            if _sr.get("successful"):
                                sr = _sr
                                break

                        if sr:
                            sprints = (sr.get("data", {}).get("values") or
                                       sr.get("data", {}).get("sprints") or [])
                            for sp in sprints:
                                name  = sp.get("name", "")
                                sid   = sp.get("id")
                                state = sp.get("state", "unknown")
                                if name and sid:
                                    sprint_map[name] = sid
                                    sprint_state_map[name] = state
                                    print(f"[AGENT 3]   Sprint: '{name}' id={sid} state={state}")
                                if state == "active" and sid:
                                    active_sprint_id = sid
                            print(f"[AGENT 3] Sprint map: {sprint_map} | active_id={active_sprint_id}")
        except Exception as e:
            print(f"[AGENT 3] Sprint pre-fetch error (non-fatal): {e}")
            import traceback; traceback.print_exc()

    # Helper: get sprint ID for a name, creating + activating it if needed
    _created_sprint_ids: dict = {}

    def ensure_sprint(sprint_name: str):
        nonlocal active_sprint_id
        # Already known
        if sprint_name in sprint_map:
            return sprint_map[sprint_name]
        if sprint_name in _created_sprint_ids:
            return _created_sprint_ids[sprint_name]
        # Fall back to any active sprint
        if active_sprint_id:
            return active_sprint_id
        # Try to create one
        if not create_sprint_slug or not board_id:
            return None
        from datetime import datetime, timedelta
        now = datetime.utcnow()
        try:
            cr = composio_direct.tools.execute(
                slug=create_sprint_slug,
                arguments={
                    "name": sprint_name,
                    "originBoardId": board_id,
                    "startDate": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "endDate": (now + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                },
                user_id=COMPOSIO_USER_ID,
                dangerously_skip_version_check=True,
            )
            if cr.get("successful"):
                sid = cr.get("data", {}).get("id")
                if sid:
                    print(f"[AGENT 3]   Created sprint '{sprint_name}' (id={sid})")
                    sprint_map[sprint_name] = sid
                    _created_sprint_ids[sprint_name] = sid
                    # Activate it so it appears on the board
                    if update_sprint_slug:
                        ur = composio_direct.tools.execute(
                            slug=update_sprint_slug,
                            arguments={"sprintId": sid, "state": "active"},
                            user_id=COMPOSIO_USER_ID,
                            dangerously_skip_version_check=True,
                        )
                        if ur.get("successful"):
                            active_sprint_id = sid
                            print(f"[AGENT 3]   Sprint '{sprint_name}' activated ✓")
                        else:
                            print(f"[AGENT 3]   Sprint activate failed: {str(ur.get('error',''))[:100]}")
                    return sid
            else:
                print(f"[AGENT 3]   Sprint create failed: {str(cr.get('error',''))[:100]}")
        except Exception as e:
            print(f"[AGENT 3]   Sprint create error: {e}")
        return None

    # ── Transition cache (fetched once per issue, keyed by status name) ──────
    _trans_cache: dict = {}

    def get_transition_id(issue_key: str, target: str):
        if not get_trans_slug or not transition_slug:
            return None
        if issue_key not in _trans_cache:
            try:
                tr = composio_direct.tools.execute(
                    slug=get_trans_slug,
                    arguments={"issue_id_or_key": issue_key},
                    user_id=COMPOSIO_USER_ID,
                    dangerously_skip_version_check=True,
                )
                data = tr.get("data", {})
                # Composio may nest under transitions, values, or return a list directly
                raw = (data.get("transitions") or data.get("values")
                       or data.get("results") or data or [])
                transitions = raw if isinstance(raw, list) else []
                print(f"[AGENT 3]   Available transitions for {issue_key}: {[t.get('name') for t in transitions]}")
                _trans_cache[issue_key] = transitions
            except Exception as e:
                print(f"[AGENT 3]   Transition fetch error: {e}")
                _trans_cache[issue_key] = []
        for t in _trans_cache[issue_key]:
            if t.get("name", "").lower() == target.lower():
                return t.get("id")
        return None

    # ── Main processing loop ─────────────────────────────────────────────────
    created = 0
    empty_polls = 0
    max_empty_polls = 2

    while True:
        event = None
        try:
            try:
                event = await create_queue.dequeue_event(no_wait=True)
                empty_polls = 0
                print(f"[AGENT 3] Received event: {event.get('type') if event else 'None'}")
            except RuntimeError:
                await asyncio.sleep(2)
                event = None
                empty_polls += 1
                print(f"[AGENT 3] No events (poll {empty_polls}/{max_empty_polls})")
        except Exception as e:
            print(f"[AGENT 3] Dequeue error: {e}")
            continue

        if not event:
            if empty_polls >= max_empty_polls:
                print(f"\n[AGENT 3] Finished! Created {created} Jira tickets\n")
                break
            continue

        try:
            if event.get("type") != "CreateJiraTicket":
                print(f"[AGENT 3] Unknown event type: {event.get('type')}")
                continue

            payload  = event["payload"]
            row_data = payload["row_data"]
            description = payload["description"]

            sprint_name  = row_data.get("sprint", "")
            status_raw   = row_data.get("status", "To Do").strip()
            assignee     = row_data.get("assignee", "").strip()
            owner        = row_data.get("owner", "").strip()
            effort_raw   = row_data.get("effort", "").strip()
            priority_idx = min(row_data.get("priority", 2), 4)
            priority_name = PRIORITY_NAMES[priority_idx]

            print(f"\n[AGENT 3] Creating: {row_data['title'][:60]}...")
            print(f"[AGENT 3]   priority={priority_name} | sprint={sprint_name} | status={status_raw} | assignee={assignee or owner} | points={effort_raw}")

            # ── Build create payload ────────────────────────────────────────
            create_args = {
                "projectKey": JIRA_PROJECT_KEY,
                "summary":    row_data["title"][:255],
                "issueType":  row_data.get("type") or "Task",
                "description": description,
                "priority":   priority_name,
                "labels":     ["sprint-automation"],
            }

            # Story points — included in create; also retried via edit after creation
            story_points_val = None
            if effort_raw:
                try:
                    story_points_val = float(effort_raw)
                    create_args["story_points"] = story_points_val
                except ValueError:
                    pass

            # Assignee — prefer the Assignee column, fall back to Owner
            # Jira rejects display names in "assignee"; use "assignee_name" instead
            actor = assignee or owner
            if actor:
                create_args["assignee_name"] = actor

            # ── Create the issue ────────────────────────────────────────────
            result = composio_direct.tools.execute(
                slug=create_slug,
                arguments=create_args,
                user_id=COMPOSIO_USER_ID,
                dangerously_skip_version_check=True,
            )
            print(f"[AGENT 3] Raw result: {str(result)[:400]}")

            if not result.get("successful"):
                err_str = str(result.get("error", ""))
                if actor and ("assignee" in err_str.lower() or "account" in err_str.lower() or "user" in err_str.lower()):
                    print(f"[AGENT 3]   Assignee field rejected — retrying without it (will assign separately)")
                    create_args.pop("assignee_name", None)
                    result = composio_direct.tools.execute(
                        slug=create_slug,
                        arguments=create_args,
                        user_id=COMPOSIO_USER_ID,
                        dangerously_skip_version_check=True,
                    )
                if not result.get("successful"):
                    print(f"[AGENT 3] Failed: {result.get('error', result)}")
                    continue

            created += 1
            issue_key = result.get("data", {}).get("key", "unknown")
            print(f"[AGENT 3] Created: {issue_key} - {row_data['title'][:50]}")

            # ── Assign issue (separate call — more reliable than inline field) ──
            if actor and assign_slug:
                try:
                    ar = composio_direct.tools.execute(
                        slug=assign_slug,
                        arguments={"issue_id_or_key": issue_key, "assignee_name": actor},
                        user_id=COMPOSIO_USER_ID,
                        dangerously_skip_version_check=True,
                    )
                    if ar.get("successful"):
                        print(f"[AGENT 3]   Assignee → '{actor}' ✓")
                    else:
                        print(f"[AGENT 3]   Assignee failed: {str(ar.get('error',''))[:200]}")
                except Exception as ae:
                    print(f"[AGENT 3]   Assignee error: {ae}")

            # ── Story points (post-creation edit — create field is often ignored) ──
            if story_points_val is not None and edit_issue_slug and issue_key != "unknown":
                try:
                    # Composio JIRA_EDIT_ISSUE requires `fields` as a JSON string, not a dict
                    for sp_args in [
                        {"issue_id_or_key": issue_key, "story_point_estimate": story_points_val},
                        {"issue_id_or_key": issue_key, "fields": json.dumps({"customfield_10016": story_points_val})},
                        {"issue_id_or_key": issue_key, "fields": json.dumps({"customfield_10028": story_points_val})},
                    ]:
                        spr = composio_direct.tools.execute(
                            slug=edit_issue_slug,
                            arguments=sp_args,
                            user_id=COMPOSIO_USER_ID,
                            dangerously_skip_version_check=True,
                        )
                        if spr.get("successful"):
                            print(f"[AGENT 3]   Story points → {story_points_val} ✓")
                            break
                    else:
                        print(f"[AGENT 3]   Story points update failed: {str(spr.get('error',''))[:150]}")
                except Exception as spe:
                    print(f"[AGENT 3]   Story points error: {spe}")

            # ── Move to sprint ──────────────────────────────────────────────
            sprint_id = ensure_sprint(sprint_name) if sprint_name else active_sprint_id
            sprint_assigned = False
            if sprint_id:
                if move_sprint_slug:
                    # JIRA_MOVE_ISSUE_TO_SPRINT requires sprintId + issues array
                    for mv_args in [
                        {"sprintId": sprint_id, "issues": [issue_key]},
                        {"sprint_id": sprint_id, "issues": [issue_key]},
                    ]:
                        sp_res = composio_direct.tools.execute(
                            slug=move_sprint_slug,
                            arguments=mv_args,
                            user_id=COMPOSIO_USER_ID,
                            dangerously_skip_version_check=True,
                        )
                        if sp_res.get("successful"):
                            print(f"[AGENT 3]   Sprint → '{sprint_name or 'active'}' ✓")
                            sprint_assigned = True
                            # If sprint is "future", start it so tickets appear on the board
                            sprint_state = sprint_state_map.get(sprint_name, "")
                            if sprint_state == "future" and update_sprint_slug:
                                from datetime import datetime, timedelta
                                now = datetime.utcnow()
                                ur = composio_direct.tools.execute(
                                    slug=update_sprint_slug,
                                    arguments={
                                        "sprintId": sprint_id,
                                        "state": "active",
                                        "startDate": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                                        "endDate": (now + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                                    },
                                    user_id=COMPOSIO_USER_ID,
                                    dangerously_skip_version_check=True,
                                )
                                if ur.get("successful"):
                                    print(f"[AGENT 3]   Sprint '{sprint_name}' activated — now visible on board ✓")
                                    sprint_state_map[sprint_name] = "active"
                                else:
                                    print(f"[AGENT 3]   Sprint activate failed: {str(ur.get('error',''))[:150]}")
                                    print(f"[AGENT 3]   ⚠ Sprint is in 'future' state — go to Jira and start it manually to see tickets on the board")
                            elif sprint_state == "future":
                                print(f"[AGENT 3]   ⚠ Sprint '{sprint_name}' is in 'future' state — start it in Jira to see tickets on the board")
                            break
                        print(f"[AGENT 3]   Sprint move attempt ({list(mv_args.keys())}): {str(sp_res.get('error',''))[:100]}")
                    if not sprint_assigned:
                        print(f"[AGENT 3]   Sprint move failed — trying edit fallback")

                if not sprint_assigned and edit_issue_slug:
                    # Fallback: set sprint via edit/update issue using Jira's sprint custom field
                    sp_res2 = composio_direct.tools.execute(
                        slug=edit_issue_slug,
                        arguments={
                            "issue_id_or_key": issue_key,
                            "fields": {"customfield_10020": {"id": sprint_id}},
                        },
                        user_id=COMPOSIO_USER_ID,
                        dangerously_skip_version_check=True,
                    )
                    if sp_res2.get("successful"):
                        print(f"[AGENT 3]   Sprint (via edit) → '{sprint_name or 'active'}' ✓")
                        sprint_assigned = True
                    else:
                        print(f"[AGENT 3]   Sprint edit failed: {str(sp_res2.get('error',''))[:200]}")

                if not sprint_assigned:
                    print(f"[AGENT 3]   Could not assign sprint — ticket stays in backlog")
            else:
                print(f"[AGENT 3]   No sprint ID found for '{sprint_name}' — ticket stays in backlog")

            # ── Transition status ───────────────────────────────────────────
            target_status = STATUS_MAP.get(status_raw.lower(), "")
            if target_status and target_status.lower() != "to do":
                trans_id = get_transition_id(issue_key, target_status)
                if trans_id:
                    tr_res = composio_direct.tools.execute(
                        slug=transition_slug,
                        arguments={"issue_id_or_key": issue_key, "transition_id": trans_id},
                        user_id=COMPOSIO_USER_ID,
                        dangerously_skip_version_check=True,
                    )
                    if tr_res.get("successful"):
                        print(f"[AGENT 3]   Status → '{target_status}' ✓")
                    else:
                        print(f"[AGENT 3]   Transition failed: {str(tr_res.get('error',''))[:150]}")
                else:
                    print(f"[AGENT 3]   No transition found for '{target_status}' (get_trans_slug={get_trans_slug})")

        except Exception as e:
            print(f"[AGENT 3] Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            create_queue.task_done()


# ===== Main Orchestrator =====
async def main():
    print("\n" + "=" * 70)
    print("🚀 A2A Sprint Planning System Starting")
    print(f"   Sheet: {SPREADSHEET_ID}")
    print(f"   Jira Project: {JIRA_PROJECT_KEY}")
    print("=" * 70)

    redis_client = aioredis.from_url(REDIS_URL, max_connections=10, decode_responses=True)
    queue_manager = _QueueManager(redis_client, prefix=QUEUE_PREFIX)

    enrich_queue = await queue_manager.create_or_tap(QUEUE_ENRICH)
    create_queue = await queue_manager.create_or_tap(QUEUE_CREATE)

    print(f"[OK] Redis queues ready: {QUEUE_PREFIX}{QUEUE_ENRICH}, {QUEUE_PREFIX}{QUEUE_CREATE}\n")

    await sprint_reader_agent(enrich_queue)
    await context_enricher_agent(enrich_queue, create_queue)
    await jira_creator_agent(create_queue)

    print("\n" + "=" * 70)
    print("✨ All agents completed successfully!")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
