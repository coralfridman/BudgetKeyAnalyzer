import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import streamlit as st
import streamlit.components.v1 as components

DATA_FILE = Path(__file__).with_name("poll_winner_data.json")

HE = {
    "title": "\u05d1\u05d5\u05e0\u05d9\u05dd \u05e1\u05e7\u05e8, \u05de\u05e9\u05ea\u05e4\u05d9\u05dd QR, \u05d5\u05e8\u05d5\u05d0\u05d9\u05dd \u05de\u05e0\u05e6\u05d7.",
    "new": "\u05e1\u05e7\u05e8 \u05d7\u05d3\u05e9", "question": "\u05e9\u05d0\u05dc\u05d4", "answers": "\u05ea\u05e9\u05d5\u05d1\u05d5\u05ea",
    "answer": "\u05ea\u05e9\u05d5\u05d1\u05d4", "multi": "\u05d0\u05e4\u05e9\u05e8 \u05dc\u05d1\u05d7\u05d5\u05e8 \u05db\u05de\u05d4 \u05ea\u05e9\u05d5\u05d1\u05d5\u05ea",
    "one": "\u05d4\u05e6\u05d1\u05e2\u05d4 \u05d0\u05d7\u05ea \u05dc\u05db\u05dc \u05e9\u05dd", "names": "\u05dc\u05d4\u05e6\u05d9\u05d2 \u05e9\u05de\u05d5\u05ea \u05de\u05e6\u05d1\u05d9\u05e2\u05d9\u05dd",
    "create": "\u05d9\u05e6\u05d9\u05e8\u05ea \u05e1\u05e7\u05e8", "share": "\u05e7\u05d9\u05e9\u05d5\u05e8 \u05dc\u05e9\u05d9\u05ea\u05d5\u05e3", "admin": "\u05e0\u05d9\u05d4\u05d5\u05dc",
    "name": "\u05d4\u05e9\u05dd \u05e9\u05dc\u05da", "vote": "\u05d4\u05e6\u05d1\u05e2\u05d4", "results": "\u05ea\u05d5\u05e6\u05d0\u05d5\u05ea",
    "winner": "\u05de\u05e0\u05e6\u05d7", "tie": "\u05ea\u05d9\u05e7\u05d5", "waiting": "\u05de\u05d7\u05db\u05d9\u05dd \u05dc\u05d4\u05e6\u05d1\u05e2\u05d5\u05ea",
    "votes": "\u05d4\u05e6\u05d1\u05e2\u05d5\u05ea", "save": "\u05e9\u05de\u05d9\u05e8\u05d4", "saved": "\u05e0\u05e9\u05de\u05e8", "missing": "\u05e6\u05e8\u05d9\u05da \u05e9\u05dd \u05d5\u05ea\u05e9\u05d5\u05d1\u05d4", "dup": "\u05d4\u05e9\u05dd \u05db\u05d1\u05e8 \u05d4\u05e6\u05d1\u05d9\u05e2", "refresh": "\u05e8\u05e2\u05e0\u05d5\u05df"
}
EN = {"title":"Build a poll, share a QR, and watch the winner.","new":"New poll","question":"Question","answers":"Answers","answer":"Answer","multi":"Allow multiple answers","one":"One vote per name","names":"Show voter names","create":"Create poll","share":"Share link","admin":"Admin","name":"Your name","vote":"Vote","results":"Results","winner":"Winner","tie":"Tie","waiting":"Waiting for votes","votes":"votes","save":"Save","saved":"Saved","missing":"Name and answer required","dup":"That name already voted","refresh":"Refresh"}

st.set_page_config(page_title="Poll Winner", page_icon="PW", layout="wide")

if "lang" not in st.session_state:
    st.session_state.lang = "he"
lang = st.sidebar.radio("Language / \u05e9\u05e4\u05d4", ["he", "en"], index=0 if st.session_state.lang == "he" else 1)
st.session_state.lang = lang
TXT = HE if lang == "he" else EN
if lang == "he":
    st.markdown("<style>html,body,[data-testid='stAppViewContainer']{direction:rtl;text-align:right}</style>", unsafe_allow_html=True)

def t(k): return TXT[k]
def now(): return datetime.now(timezone.utc).isoformat()
def load():
    if not DATA_FILE.exists(): return {"polls": {}}
    return json.loads(DATA_FILE.read_text(encoding="utf-8"))
def save(data): DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
def base_url():
    try: return st.context.url.split("?")[0]
    except Exception: return ""
def make_url(**params): return base_url() + "?" + urlencode(params)
def qr(url):
    src = "https://api.qrserver.com/v1/create-qr-code/?" + urlencode({"size":"280x280", "data":url})
    components.html(f'<img src="{src}" style="width:280px;height:280px;border:1px solid #ddd;border-radius:8px;padding:8px;background:white">', height=305)
def autorefresh():
    components.html("<script>setTimeout(()=>window.parent.location.reload(),5000)</script>", height=0)

def stats(poll):
    counts = {o["id"]: 0 for o in poll["options"]}
    voters = {o["id"]: [] for o in poll["options"]}
    for v in poll["votes"]:
        for oid in v["option_ids"]:
            if oid in counts:
                counts[oid] += 1
                voters[oid].append(v["name"])
    top = max(counts.values(), default=0)
    winners = [] if top == 0 else [oid for oid, c in counts.items() if c == top]
    return counts, voters, winners

def show_results(poll, admin=False):
    counts, voters, winners = stats(poll)
    st.subheader(t("results"))
    names = [o["text"] for o in poll["options"] if o["id"] in winners]
    if names:
        st.success((t("tie") if len(names) > 1 else t("winner")) + ": " + ", ".join(names))
    else:
        st.info(t("waiting"))
    st.metric(t("votes"), len(poll["votes"]))
    max_votes = max(1, *counts.values())
    for o in poll["options"]:
        label = f'{o["text"]} · {counts[o["id"]]} {t("votes")}'
        if admin or poll["settings"].get("show_names"):
            people = ", ".join(voters[o["id"]])
            if people: label += " · " + people
        st.progress(counts[o["id"]] / max_votes, text=label)

def builder(data):
    st.title(t("title"))
    with st.form("builder"):
        question = st.text_input(t("question"), placeholder="What should we eat? / \u05de\u05d4 \u05e0\u05d0\u05db\u05dc?")
        n = st.number_input(t("answers"), min_value=2, max_value=12, value=3)
        answers = [st.text_input(f'{t("answer")} {i+1}') for i in range(n)]
        multi = st.checkbox(t("multi"), True)
        one = st.checkbox(t("one"), True)
        names = st.checkbox(t("names"), False)
        submitted = st.form_submit_button(t("create"), type="primary")
    if submitted:
        answers = [a.strip() for a in answers if a.strip()]
        if not question.strip() or len(answers) < 2:
            st.error(t("missing")); return
        pid, token = uuid.uuid4().hex[:8], uuid.uuid4().hex
        data["polls"][pid] = {"id": pid, "token": token, "question": question.strip(), "options": [{"id": uuid.uuid4().hex[:8], "text": a} for a in answers], "settings": {"multi": multi, "one": one, "show_names": names, "lang": lang}, "votes": [], "created": now()}
        save(data)
        st.success(t("saved"))
        st.text_input(t("share"), make_url(poll=pid), disabled=True)
        st.text_input(t("admin"), make_url(admin=pid, token=token), disabled=True)
        qr(make_url(poll=pid))

def vote_view(data, pid):
    poll = data["polls"].get(pid)
    if not poll: st.error("Poll not found"); return
    st.title(poll["question"])
    with st.form("vote_form"):
        name = st.text_input(t("name"))
        options = {o["text"]: o["id"] for o in poll["options"]}
        if poll["settings"].get("multi"):
            chosen = st.multiselect(t("answers"), list(options))
            ids = [options[c] for c in chosen]
        else:
            chosen = st.radio(t("answers"), list(options), index=None)
            ids = [options[chosen]] if chosen else []
        submitted = st.form_submit_button(t("vote"), type="primary")
    if submitted:
        if not name.strip() or not ids: st.error(t("missing")); return
        if poll["settings"].get("one") and any(v["name"].casefold() == name.strip().casefold() for v in poll["votes"]):
            st.error(t("dup")); return
        poll["votes"].append({"id": uuid.uuid4().hex[:10], "name": name.strip(), "option_ids": ids, "at": now()})
        save(data); st.success(t("saved"))
    if st.button(t("refresh")): st.rerun()
    show_results(poll)
    autorefresh()

def admin_view(data, pid, token):
    poll = data["polls"].get(pid)
    if not poll or poll.get("token") != token: st.error("Not found"); return
    st.title(poll["question"])
    cols = st.columns([1, 2])
    with cols[0]: qr(make_url(poll=pid))
    with cols[1]:
        st.text_input(t("share"), make_url(poll=pid), disabled=True)
        st.text_input(t("admin"), make_url(admin=pid, token=token), disabled=True)
    with st.form("settings"):
        poll["settings"]["multi"] = st.checkbox(t("multi"), poll["settings"].get("multi", True))
        poll["settings"]["one"] = st.checkbox(t("one"), poll["settings"].get("one", True))
        poll["settings"]["show_names"] = st.checkbox(t("names"), poll["settings"].get("show_names", False))
        if st.form_submit_button(t("save"), type="primary"):
            save(data); st.success(t("saved"))
    if st.button(t("refresh")): st.rerun()
    show_results(poll, admin=True)
    autorefresh()

data = load()
query = st.query_params
if "poll" in query:
    vote_view(data, query["poll"])
elif "admin" in query and "token" in query:
    admin_view(data, query["admin"], query["token"])
else:
    builder(data)
