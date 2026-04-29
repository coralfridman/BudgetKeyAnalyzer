const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, "data.json");
const PUBLIC_DIR = path.join(__dirname, "public");

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml; charset=utf-8"
};

const state = {
  polls: new Map(),
  streams: new Map()
};

loadState();

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (req.method === "GET" && url.pathname === "/api/health") {
      return json(res, 200, { ok: true });
    }

    if (req.method === "POST" && url.pathname === "/api/polls") {
      const body = await readJson(req);
      return createPoll(req, res, body);
    }

    const pollMatch = url.pathname.match(/^\/api\/polls\/([^/]+)$/);
    if (req.method === "GET" && pollMatch) {
      return getPoll(res, pollMatch[1], url.searchParams.get("admin"));
    }

    const voteMatch = url.pathname.match(/^\/api\/polls\/([^/]+)\/vote$/);
    if (req.method === "POST" && voteMatch) {
      const body = await readJson(req);
      return vote(res, voteMatch[1], body);
    }

    const settingsMatch = url.pathname.match(/^\/api\/polls\/([^/]+)\/settings$/);
    if (req.method === "PATCH" && settingsMatch) {
      const body = await readJson(req);
      return updateSettings(res, settingsMatch[1], body);
    }

    const streamMatch = url.pathname.match(/^\/api\/polls\/([^/]+)\/stream$/);
    if (req.method === "GET" && streamMatch) {
      return streamPoll(req, res, streamMatch[1]);
    }

    return serveStatic(res, url.pathname);
  } catch (error) {
    console.error(error);
    return json(res, 500, { error: "server_error" });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`Poll Winner is running on ${HOST}:${PORT}`);
});

function createPoll(req, res, body) {
  const question = cleanText(body.question, 160);
  const options = Array.isArray(body.options)
    ? body.options.map((item) => cleanText(item, 80)).filter(Boolean)
    : [];

  if (!question || options.length < 2) {
    return json(res, 400, { error: "invalid_poll" });
  }

  const id = crypto.randomUUID().slice(0, 8);
  const adminToken = crypto.randomUUID().replace(/-/g, "");
  const now = new Date().toISOString();
  const poll = {
    id,
    adminToken,
    question,
    options: options.map((text) => ({ id: crypto.randomUUID().slice(0, 8), text })),
    settings: {
      allowMultipleAnswers: body.allowMultipleAnswers !== false,
      oneVotePerName: body.oneVotePerName !== false,
      showVoters: body.showVoters === true,
      language: body.language === "en" ? "en" : "he"
    },
    votes: [],
    createdAt: now,
    updatedAt: now
  };

  state.polls.set(id, poll);
  saveState();
  broadcast(id);

  const origin = getOrigin(req);
  return json(res, 201, {
    id,
    adminToken,
    pollUrl: `${origin}/?poll=${id}`,
    adminUrl: `${origin}/?admin=${id}&token=${adminToken}`
  });
}

function getPoll(res, id, adminToken) {
  const poll = state.polls.get(id);
  if (!poll) return json(res, 404, { error: "not_found" });
  return json(res, 200, serializePoll(poll, adminToken === poll.adminToken));
}

function vote(res, id, body) {
  const poll = state.polls.get(id);
  if (!poll) return json(res, 404, { error: "not_found" });

  const voterName = cleanText(body.voterName, 60);
  const optionIds = Array.isArray(body.optionIds) ? body.optionIds : [];
  const validOptionIds = new Set(poll.options.map((option) => option.id));
  const selected = [...new Set(optionIds)].filter((optionId) => validOptionIds.has(optionId));

  if (!voterName || selected.length === 0) {
    return json(res, 400, { error: "invalid_vote" });
  }

  if (!poll.settings.allowMultipleAnswers && selected.length > 1) {
    return json(res, 400, { error: "single_answer_only" });
  }

  if (poll.settings.oneVotePerName) {
    const normalized = voterName.trim().toLocaleLowerCase();
    const alreadyVoted = poll.votes.some(
      (voteItem) => voteItem.voterName.trim().toLocaleLowerCase() === normalized
    );
    if (alreadyVoted) {
      return json(res, 409, { error: "already_voted" });
    }
  }

  poll.votes.push({
    id: crypto.randomUUID().slice(0, 10),
    voterName,
    optionIds: selected,
    createdAt: new Date().toISOString()
  });
  poll.updatedAt = new Date().toISOString();
  saveState();
  broadcast(id);

  return json(res, 201, { ok: true, poll: serializePoll(poll, false) });
}

function updateSettings(res, id, body) {
  const poll = state.polls.get(id);
  if (!poll) return json(res, 404, { error: "not_found" });
  if (body.adminToken !== poll.adminToken) return json(res, 403, { error: "forbidden" });

  poll.settings.allowMultipleAnswers = body.allowMultipleAnswers === true;
  poll.settings.oneVotePerName = body.oneVotePerName === true;
  poll.settings.showVoters = body.showVoters === true;
  poll.settings.language = body.language === "en" ? "en" : "he";
  poll.updatedAt = new Date().toISOString();
  saveState();
  broadcast(id);

  return json(res, 200, { ok: true, poll: serializePoll(poll, true) });
}

function streamPoll(req, res, id) {
  if (!state.polls.has(id)) {
    return json(res, 404, { error: "not_found" });
  }

  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no"
  });
  res.write(`event: poll\ndata: ${JSON.stringify(serializePoll(state.polls.get(id), false))}\n\n`);

  if (!state.streams.has(id)) state.streams.set(id, new Set());
  state.streams.get(id).add(res);

  req.on("close", () => {
    const streams = state.streams.get(id);
    if (streams) streams.delete(res);
  });
}

function broadcast(id) {
  const streams = state.streams.get(id);
  const poll = state.polls.get(id);
  if (!streams || !poll) return;
  const payload = `event: poll\ndata: ${JSON.stringify(serializePoll(poll, false))}\n\n`;
  for (const res of streams) res.write(payload);
}

function serializePoll(poll, isAdmin) {
  const counts = new Map(poll.options.map((option) => [option.id, 0]));
  for (const voteItem of poll.votes) {
    for (const optionId of voteItem.optionIds) {
      counts.set(optionId, (counts.get(optionId) || 0) + 1);
    }
  }

  const maxVotes = Math.max(0, ...counts.values());
  const winners = maxVotes === 0
    ? []
    : poll.options.filter((option) => counts.get(option.id) === maxVotes).map((option) => option.id);

  return {
    id: poll.id,
    question: poll.question,
    options: poll.options.map((option) => ({
      ...option,
      votes: counts.get(option.id) || 0,
      voters: poll.settings.showVoters || isAdmin
        ? poll.votes
            .filter((voteItem) => voteItem.optionIds.includes(option.id))
            .map((voteItem) => voteItem.voterName)
        : []
    })),
    settings: poll.settings,
    votesCount: poll.votes.length,
    winners,
    createdAt: poll.createdAt,
    updatedAt: poll.updatedAt,
    isAdmin
  };
}

function serveStatic(res, requestedPath) {
  let filePath = requestedPath === "/" ? "/index.html" : requestedPath;
  filePath = path.normalize(filePath).replace(/^(\.\.[/\\])+/, "");
  const fullPath = path.join(PUBLIC_DIR, filePath);

  if (!fullPath.startsWith(PUBLIC_DIR)) {
    return json(res, 403, { error: "forbidden" });
  }

  fs.readFile(fullPath, (error, data) => {
    if (error) {
      fs.readFile(path.join(PUBLIC_DIR, "index.html"), (fallbackError, fallbackData) => {
        if (fallbackError) return json(res, 404, { error: "not_found" });
        res.writeHead(200, { "Content-Type": mimeTypes[".html"] });
        res.end(fallbackData);
      });
      return;
    }

    const extension = path.extname(fullPath);
    res.writeHead(200, { "Content-Type": mimeTypes[extension] || "application/octet-stream" });
    res.end(data);
  });
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 1_000_000) {
        req.destroy();
        reject(new Error("payload_too_large"));
      }
    });
    req.on("end", () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch (error) {
        reject(error);
      }
    });
  });
}

function json(res, status, payload) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload));
}

function cleanText(value, maxLength) {
  return String(value || "").trim().slice(0, maxLength);
}

function getOrigin(req) {
  const protocol = req.headers["x-forwarded-proto"] || "http";
  return `${protocol}://${req.headers.host}`;
}

function loadState() {
  if (!fs.existsSync(DATA_FILE)) return;
  const parsed = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  for (const poll of parsed.polls || []) {
    state.polls.set(poll.id, poll);
  }
}

function saveState() {
  const payload = { polls: [...state.polls.values()] };
  fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(payload, null, 2));
}
