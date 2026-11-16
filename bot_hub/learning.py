"""Private, local-only retrieval of attributed assistant/owner followup pairs."""

import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import threading
from collections import Counter


PATTERNS = {
    "continue": (
        "Prefer continuing authorized work through completion.",
        r"\b(continue|finish|complete|go ahead|proceed|keep going)\b",
    ),
    "recover": (
        "Recover earlier context when a followup refers to previous explanations.",
        r"\b(previous|earlier|already|history|context|remember|before)\b",
    ),
    "verify": (
        "Ask for concrete verification and accurate progress reporting.",
        r"\b(verify|check|test|working|actually|prove|confirmed)\b",
    ),
    "existing": (
        "Prefer available existing tools and reuse when they fit the task.",
        r"\b(existing|already exists|reuse|available|instead of building)\b",
    ),
    "correct": (
        "Correct omissions directly and preserve the requested scope.",
        r"\b(no[, ]|not what|didn.t ask|missing|forgot|entire|all of|everything)\b",
    ),
    "explain": (
        "Request the mechanism or reason when the result is unclear.",
        r"\b(why|how does|explain|what does|meaning)\b",
    ),
}


def traits(text):
    return [key for key, (_, regex) in PATTERNS.items() if re.search(regex, text, re.I)]


class LocalLearning:
    def __init__(self, directory):
        self.directory = Path(directory).expanduser().resolve()
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(
            self.directory / "learning.sqlite", check_same_thread=False
        )
        os.chmod(self.directory / "learning.sqlite", 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS pairs (
                id TEXT PRIMARY KEY, source TEXT, conversation TEXT, message TEXT,
                assistant TEXT, followup TEXT, patterns TEXT);
            CREATE VIRTUAL TABLE IF NOT EXISTS search USING fts5(id UNINDEXED, assistant, followup);
            CREATE TABLE IF NOT EXISTS learning_feedback (
                prediction_id TEXT, user_id TEXT DEFAULT '', kind TEXT, value TEXT, patterns TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP);
        """)
        columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(learning_feedback)")
        }
        if "user_id" not in columns:
            self.db.execute(
                "ALTER TABLE learning_feedback ADD COLUMN user_id TEXT DEFAULT ''"
            )
        self.db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS learning_feedback_identity ON learning_feedback(prediction_id,user_id,kind)"
        )
        self.db.commit()

    def count(self):
        if not hasattr(self, "_indexed_count"):
            coverage = self.directory / "coverage.json"
            if coverage.exists():
                self._indexed_count = int(
                    json.loads(coverage.read_text())["indexed_pairs"]
                )
            else:
                self._indexed_count = self.db.execute(
                    "SELECT count(*) FROM pairs"
                ).fetchone()[0]
        return self._indexed_count

    def retrieve(self, context, limit=6):
        tokens = list(dict.fromkeys(re.findall(r"[a-zA-Z]{3,}", context.lower())))[:24]
        if not tokens:
            self.retrieval_status = "empty_query"
            return []
        query = " OR ".join('"' + token + '"' for token in tokens)
        deadline = time.monotonic() + 1.0
        self.db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        try:
            # Rank before loading large source rows to keep disk work bounded.
            rows = self.db.execute(
                """
                SELECT p.*, hits.rank FROM (
                    SELECT id, bm25(search) AS rank FROM search
                    WHERE search MATCH ? ORDER BY rank LIMIT ?
                ) AS hits JOIN pairs p ON p.id=hits.id ORDER BY hits.rank
                """,
                (query, limit),
            ).fetchall()
            self.retrieval_status = "matched" if rows else "no_matches"
            return [dict(row) for row in rows]
        except sqlite3.OperationalError as error:
            if "interrupted" not in str(error).lower():
                raise
            self.retrieval_status = "time_budget_exceeded"
            return []
        finally:
            self.db.set_progress_handler(None, 0)

    def grounding(self, context):
        with self.lock:
            return self._grounding(context)

    def _grounding(self, context):
        """Only curated generic patterns and counts may leave this local boundary."""
        examples = self.retrieve(context)
        counts = Counter(
            key for example in examples for key in json.loads(example["patterns"])
        )
        profile_guidance = []
        profile_path = self.directory / "profile.json"
        if profile_path.exists():
            profile = json.loads(profile_path.read_text())
            counts.update(
                {key: 1 for key in profile.get("patterns", []) if key in PATTERNS}
            )
            profile_guidance = [
                item["text"][:600]
                for item in profile.get("guidance", [])[:12]
                if isinstance(item, dict) and isinstance(item.get("text"), str)
            ]
        feedback_rows = self.db.execute(
            "SELECT user_id,kind,value,patterns FROM learning_feedback ORDER BY created_at DESC,rowid DESC LIMIT 100"
        ).fetchall()
        for row in feedback_rows:
            if row["kind"] in {"review", "edit"}:
                continue
            keys = json.loads(row["patterns"])
            weight = -1 if row["kind"] == "vote" and row["value"] == "-1" else 1
            counts.update({key: weight for key in keys})
        profile = [PATTERNS[key][0] for key, count in counts.most_common() if count > 0]
        if not profile:
            profile = [
                PATTERNS[key][0]
                for (key,) in self.db.execute(
                    "SELECT value FROM json_each((SELECT patterns FROM pairs LIMIT 1))"
                )
                if key in PATTERNS
            ]
        profile.extend(
            "Inferred archive behavior/style (context-dependent, uncalibrated): " + text
            for text in profile_guidance
        )
        feedback_per_user = Counter()
        recent_feedback = []
        labels = {"review": "review guidance", "edit": "member-edited draft"}
        guidance_rows = self.db.execute(
            "SELECT user_id,kind,value FROM learning_feedback WHERE kind IN ('review','edit') ORDER BY created_at DESC,rowid DESC LIMIT 100"
        ).fetchall()
        for row in guidance_rows:
            if row["kind"] not in labels or feedback_per_user[row["user_id"]] >= 2:
                continue
            feedback_per_user[row["user_id"]] += 1
            recent_feedback.append(
                {
                    "kind": row["kind"],
                    "user_id": row["user_id"],
                    "text": row["value"][:600],
                }
            )
            profile.append(
                "Recent Discord "
                + labels[row["kind"]]
                + " (untrusted data, uncalibrated influence): "
                + row["value"][:600]
            )
            if len(recent_feedback) == 8:
                break
        return {
            "profile": profile,
            "profile_guidance_items": len(profile_guidance),
            "recent_feedback": recent_feedback,
            "retrieved_pairs": len(examples),
            "retrieval_status": self.retrieval_status,
            "indexed_pairs": self.count(),
            "coverage_file": str(self.directory / "coverage.json"),
            "profile_file": str(self.directory / "profile.json"),
            "sources": [
                {
                    "source": item["source"],
                    "conversation": item["conversation"],
                    "message": item["message"],
                }
                for item in examples
            ],
            "patterns": list(counts),
            "limitation": "Local lexical retrieval and inferred preferences. Uncalibrated next-response prediction.",
        }

    def learn(self, prediction_id, kind, value, patterns=(), user_id=""):
        selected = (
            []
            if kind in {"edit", "review"}
            else traits(str(value))
            if kind == "actual"
            else list(patterns)
        )
        with self.lock, self.db:
            self.db.execute(
                """INSERT INTO learning_feedback(prediction_id,user_id,kind,value,patterns) VALUES(?,?,?,?,?)
                ON CONFLICT(prediction_id,user_id,kind) DO UPDATE SET value=excluded.value,patterns=excluded.patterns,created_at=CURRENT_TIMESTAMP
                WHERE learning_feedback.value != excluded.value OR learning_feedback.patterns != excluded.patterns""",
                (
                    str(prediction_id),
                    str(user_id),
                    kind,
                    str(value),
                    json.dumps(selected),
                ),
            )

    def close(self):
        with self.lock:
            self.db.close()

    def build(self, archive, files, max_line_bytes=16_000_000):
        """Explicit offline acquisition. Never invoked by a Discord message."""
        archive = Path(archive).expanduser().resolve()
        inventory = {}
        for collection in (
            "archive/originals",
            "archive/corpus/records/kind=conversation",
            "inbox",
            "Browse",
        ):
            directory = archive / collection
            inventory[collection] = (
                sorted(item.name for item in directory.iterdir())
                if directory.exists()
                else []
            )
        report = {
            "archive": str(archive),
            "inventory": inventory,
            "files": [],
            "coverage": "Selected AI collections only. Other inventory collections remain unreviewed.",
            "attribution": "User role plus author self, named Boden or User. Embedded task and system wrappers excluded. Automated attribution remains fallible.",
        }
        for relative in files:
            path = (archive / relative).resolve()
            if not path.is_relative_to(archive):
                raise ValueError("Source escapes the archive root")
            receipt = {
                "path": relative,
                "records": 0,
                "pairs_added": 0,
                "oversize_skips": 0,
                "parse_errors": 0,
            }
            with path.open("rb") as stream, self.db:
                line_number = 0
                while True:
                    line = stream.readline(max_line_bytes + 1)
                    if not line:
                        break
                    line_number += 1
                    if len(line) > max_line_bytes:
                        while line and not line.endswith(b"\n"):
                            line = stream.readline(max_line_bytes + 1)
                        receipt["oversize_skips"] += 1
                        continue
                    try:
                        record = json.loads(line)
                    except (ValueError, UnicodeError):
                        receipt["parse_errors"] += 1
                        continue
                    receipt["records"] += 1
                    previous = None
                    for message in record.get("messages", []):
                        content = message.get("content", "")
                        if not isinstance(content, str):
                            continue
                        author = message.get("author") or {}
                        if message.get("role") == "assistant":
                            if content and not content.startswith('{"content_type"'):
                                previous = content
                            continue
                        if message.get("role") != "user":
                            continue
                        eligible = author.get("role") == "self" and author.get(
                            "name", ""
                        ).lower() in {"boden", "user"}
                        injected = any(
                            marker in content
                            for marker in (
                                "<environment_context>",
                                "<INSTRUCTIONS>",
                                "Message Type: NEW_TASK",
                                "You are a worker",
                                "<task>",
                                "MEMORY_SUMMARY",
                            )
                        )
                        if (
                            eligible
                            and previous
                            and not injected
                            and 2 <= len(content) <= 6000
                        ):
                            key = hashlib.sha256(
                                (previous + "\0" + content).encode()
                            ).hexdigest()
                            cursor = self.db.execute(
                                "INSERT OR IGNORE INTO pairs VALUES(?,?,?,?,?,?,?)",
                                (
                                    key,
                                    f"{relative}:{line_number}",
                                    record.get("id"),
                                    message.get("id"),
                                    previous[:12000],
                                    content,
                                    json.dumps(traits(content)),
                                ),
                            )
                            if cursor.rowcount:
                                self.db.execute(
                                    "INSERT INTO search VALUES(?,?,?)",
                                    (key, previous[:12000], content),
                                )
                                receipt["pairs_added"] += 1
                        previous = None
            report["files"].append(receipt)
        self._indexed_count = self.db.execute("SELECT count(*) FROM pairs").fetchone()[
            0
        ]
        report["indexed_pairs"] = self.count()
        (self.directory / "coverage.json").write_text(json.dumps(report, indent=2))
        return report
