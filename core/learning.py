from __future__ import annotations

import random
import re
from difflib import SequenceMatcher
from typing import Iterable

from core.database import db


DEFAULT_LEARNED_TERMS = (
    "abrasive",
    "backwash",
    "catastrophe",
    "clownshow",
    "crooked",
    "deranged",
    "feral",
    "glitchy",
    "malfunction",
    "pathetic",
    "ragged",
    "sabotaged",
    "scrapheap",
    "shambolic",
    "trashfire",
    "unhinged",
    "vicious",
    "wreckage",
)
DEFAULT_INSULT_TEMPLATES = (
    "You're a {adj} {noun} with the tactical awareness of a wet paper bag.",
    "You sound like a {adj} {noun} trying to explain electricity to a toaster.",
    "I've seen {noun}s with more dignity than your last decision.",
    "You're not a mastermind. You're a {adj} {noun} with a confidence problem.",
)
STOPWORDS = {
    "about", "after", "again", "ain't", "already", "also", "always", "among", "another", "anyone",
    "anything", "around", "because", "before", "being", "below", "between", "bot", "could",
    "didn't", "doesn't", "doing", "don't", "every", "gonna", "guild", "hello", "here", "herself",
    "himself", "human", "into", "itself", "maybe", "message", "music", "never", "other", "should",
    "their", "there", "these", "they're", "thing", "those", "through", "under", "until", "voice",
    "wasn't", "we're", "where", "which", "while", "won't", "would", "you're", "yourself",
}
WORD_RE = re.compile(r"[a-z][a-z'-]{4,23}", re.IGNORECASE)
NORMALIZE_RE = re.compile(r"\s+")


def extract_candidate_terms(text: str, *, limit: int = 8) -> list[str]:
    seen = set()
    candidates = []
    for match in WORD_RE.findall((text or "").lower()):
        term = match.strip("'-")
        if len(term) < 5 or term in STOPWORDS or term in seen:
            continue
        if term.startswith("http"):
            continue
        seen.add(term)
        candidates.append(term)
        if len(candidates) >= limit:
            break
    return candidates


def build_dynamic_insult(target_name: str, terms: list[str]) -> str:
    clean_target = (target_name or "you").strip() or "you"
    vocabulary = list(DEFAULT_LEARNED_TERMS)
    for term in terms:
        if term not in vocabulary:
            vocabulary.append(term)

    adjective_pool = [term for term in vocabulary if len(term) >= 6]
    noun_pool = [term for term in vocabulary if len(term) >= 5]
    adj = random.choice(adjective_pool or list(DEFAULT_LEARNED_TERMS))
    noun = random.choice(noun_pool or list(DEFAULT_LEARNED_TERMS))
    template = random.choice(DEFAULT_INSULT_TEMPLATES)
    return f"{clean_target}, {template.format(adj=adj, noun=noun)}"


def normalize_phrase(text: str) -> str:
    lowered = (text or "").strip().lower()
    lowered = NORMALIZE_RE.sub(" ", lowered)
    return lowered[:500]


def compact_excerpt(text: str, *, limit: int = 180) -> str:
    cleaned = NORMALIZE_RE.sub(" ", (text or "").strip())
    return cleaned[:limit]


class LearningEngine:
    async def initialize(self) -> None:
        if not db.pool:
            return

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_learned_terms (
                        term VARCHAR(64) PRIMARY KEY,
                        source_kind VARCHAR(32) NOT NULL DEFAULT 'seed',
                        weight INT NOT NULL DEFAULT 1,
                        first_seen_user_id BIGINT NULL,
                        last_seen_user_id BIGINT NULL,
                        last_seen_guild_id BIGINT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_generated_insults (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        target_name VARCHAR(128) NOT NULL,
                        insult_text TEXT NOT NULL,
                        source_terms TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_conversation_patterns (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        pattern_type VARCHAR(32) NOT NULL,
                        normalized_input VARCHAR(500) NOT NULL,
                        normalized_output VARCHAR(500) NULL,
                        response_style VARCHAR(64) NULL,
                        first_seen_user_id BIGINT NULL,
                        last_seen_user_id BIGINT NULL,
                        last_seen_guild_id BIGINT NULL,
                        hit_count INT NOT NULL DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_pattern (pattern_type, normalized_input)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_command_learning (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        action_name VARCHAR(64) NOT NULL,
                        normalized_phrase VARCHAR(500) NOT NULL,
                        outcome VARCHAR(32) NOT NULL DEFAULT 'observed',
                        first_seen_user_id BIGINT NULL,
                        last_seen_user_id BIGINT NULL,
                        last_seen_guild_id BIGINT NULL,
                        hit_count INT NOT NULL DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_action_phrase (action_name, normalized_phrase)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_recent_context (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NULL,
                        guild_id BIGINT NULL,
                        source_kind VARCHAR(32) NOT NULL DEFAULT 'chat',
                        prompt_text TEXT NOT NULL,
                        reply_text TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_recent_context_user_guild (user_id, guild_id, id),
                        INDEX idx_recent_context_guild (guild_id, id)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_file_artifacts (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        guild_id BIGINT NULL,
                        source_kind VARCHAR(32) NOT NULL DEFAULT 'code_review',
                        filename VARCHAR(255) NULL,
                        language_hint VARCHAR(32) NULL,
                        original_code MEDIUMTEXT NOT NULL,
                        current_code MEDIUMTEXT NOT NULL,
                        offer_pending BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_file_artifacts_user_guild (user_id, guild_id, id),
                        INDEX idx_file_artifacts_offer (user_id, guild_id, offer_pending, id)
                    )
                    """
                )

                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_policy_memory (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        scope_key VARCHAR(96) NOT NULL,
                        issue_type VARCHAR(64) NOT NULL,
                        preferred_cooldown_seconds DOUBLE NOT NULL DEFAULT 45,
                        confidence_bias DOUBLE NOT NULL DEFAULT 0,
                        repair_success_count INT NOT NULL DEFAULT 0,
                        repair_failure_count INT NOT NULL DEFAULT 0,
                        notes TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_scope_issue (scope_key, issue_type)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_predictive_signals (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        scope_key VARCHAR(96) NOT NULL,
                        signal_type VARCHAR(64) NOT NULL,
                        signal_strength DOUBLE NOT NULL DEFAULT 0,
                        details_json TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_scope_signal (scope_key, signal_type, created_at)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_repair_memory (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        symptom_signature VARCHAR(255) NOT NULL,
                        repair_action VARCHAR(128) NOT NULL,
                        repair_scope VARCHAR(64) NOT NULL DEFAULT 'swarm',
                        success_count INT NOT NULL DEFAULT 0,
                        failure_count INT NOT NULL DEFAULT 0,
                        last_outcome VARCHAR(16) NOT NULL DEFAULT 'unknown',
                        last_error TEXT NULL,
                        meta_json TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_repair (symptom_signature, repair_action, repair_scope)
                    )
                    """
                )
        await self._seed_defaults()

    async def get_policy_hint(self, scope_key: str, issue_type: str) -> dict | None:
        if not db.pool:
            return None
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT preferred_cooldown_seconds, confidence_bias, repair_success_count, repair_failure_count, notes
                    FROM aria_policy_memory
                    WHERE scope_key=%s AND issue_type=%s
                    """,
                    (scope_key[:96], issue_type[:64]),
                )
                row = await cur.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            return row
        cols = ["preferred_cooldown_seconds", "confidence_bias", "repair_success_count", "repair_failure_count", "notes"]
        return dict(zip(cols, row, strict=False))

    async def update_policy_hint(self, *, scope_key: str, issue_type: str, success: bool, confidence: float, notes: str | None = None) -> None:
        if not db.pool:
            return
        bias_delta = 0.04 if success else -0.05
        cooldown = 35.0 if success else 60.0
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_policy_memory (scope_key, issue_type, preferred_cooldown_seconds, confidence_bias, repair_success_count, repair_failure_count, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        preferred_cooldown_seconds = LEAST(180, GREATEST(15, (preferred_cooldown_seconds + VALUES(preferred_cooldown_seconds)) / 2)),
                        confidence_bias = LEAST(0.25, GREATEST(-0.25, confidence_bias + VALUES(confidence_bias))),
                        repair_success_count = repair_success_count + VALUES(repair_success_count),
                        repair_failure_count = repair_failure_count + VALUES(repair_failure_count),
                        notes = COALESCE(VALUES(notes), notes)
                    """,
                    (scope_key[:96], issue_type[:64], cooldown, bias_delta, 1 if success else 0, 0 if success else 1, (notes or '')[:2000] or None),
                )

    async def record_predictive_signal(self, *, scope_key: str, signal_type: str, signal_strength: float, details_json: str | None = None) -> None:
        if not db.pool:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_predictive_signals (scope_key, signal_type, signal_strength, details_json)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (scope_key[:96], signal_type[:64], float(signal_strength), (details_json or '')[:4000] or None),
                )

    async def recent_predictive_signals(self, *, scope_key: str, signal_type: str, limit: int = 5) -> list[dict]:
        if not db.pool:
            return []
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT signal_strength, details_json, created_at
                    FROM aria_predictive_signals
                    WHERE scope_key=%s AND signal_type=%s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (scope_key[:96], signal_type[:64], int(limit)),
                )
                rows = await cur.fetchall() or []
        out=[]
        for row in rows:
            if isinstance(row, dict):
                out.append(row)
            else:
                out.append({"signal_strength": row[0], "details_json": row[1], "created_at": row[2]})
        return out

    async def _seed_defaults(self) -> None:
        if not db.pool:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for term in DEFAULT_LEARNED_TERMS:
                    await cur.execute(
                        """
                        INSERT INTO aria_learned_terms (term, source_kind, weight)
                        VALUES (%s, 'seed', 1)
                        ON DUPLICATE KEY UPDATE weight = GREATEST(weight, 1)
                        """,
                        (term,),
                    )

    async def observe_text(self, user_id: int | None, guild_id: int | None, text: str, *, source_kind: str = "message") -> list[str]:
        if not db.pool:
            return []

        terms = extract_candidate_terms(text)
        if not terms:
            return []

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for term in terms:
                    await cur.execute(
                        """
                        INSERT INTO aria_learned_terms (
                            term,
                            source_kind,
                            weight,
                            first_seen_user_id,
                            last_seen_user_id,
                            last_seen_guild_id
                        )
                        VALUES (%s, %s, 1, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            weight = weight + 1,
                            source_kind = VALUES(source_kind),
                            last_seen_user_id = VALUES(last_seen_user_id),
                            last_seen_guild_id = VALUES(last_seen_guild_id)
                        """,
                        (term, source_kind, user_id, user_id, guild_id),
                    )
        return terms

    async def record_generated_insult(self, insult_text: str, *, target_name: str, source_terms: Iterable[str]) -> None:
        if not db.pool:
            return

        source_blob = ", ".join(dict.fromkeys(source_terms))[:512]
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_generated_insults (target_name, insult_text, source_terms)
                    VALUES (%s, %s, %s)
                    """,
                    (target_name[:128], insult_text[:4000], source_blob),
                )

    async def record_conversation_pair(
        self,
        *,
        user_id: int | None,
        guild_id: int | None,
        prompt: str,
        reply: str,
        response_style: str | None = None,
    ) -> None:
        if not db.pool:
            return
        normalized_input = normalize_phrase(prompt)
        normalized_output = normalize_phrase(reply)
        if not normalized_input:
            return
        pattern_type = "conversation"
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_conversation_patterns (
                        pattern_type, normalized_input, normalized_output, response_style,
                        first_seen_user_id, last_seen_user_id, last_seen_guild_id, hit_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                    ON DUPLICATE KEY UPDATE
                        normalized_output = VALUES(normalized_output),
                        response_style = COALESCE(VALUES(response_style), response_style),
                        last_seen_user_id = VALUES(last_seen_user_id),
                        last_seen_guild_id = VALUES(last_seen_guild_id),
                        hit_count = hit_count + 1
                    """,
                    (pattern_type, normalized_input, normalized_output, response_style, user_id, user_id, guild_id),
                )

    async def record_recent_context(
        self,
        *,
        user_id: int | None,
        guild_id: int | None,
        source_kind: str,
        prompt: str,
        reply: str | None = None,
    ) -> None:
        if not db.pool or (user_id is None and guild_id is None):
            return
        prompt_text = compact_excerpt(prompt, limit=3000)
        reply_text = compact_excerpt(reply or "", limit=3000) or None
        if not prompt_text:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_recent_context (
                        user_id, guild_id, source_kind, prompt_text, reply_text
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (user_id, guild_id, source_kind[:32], prompt_text, reply_text),
                )
                await cur.execute(
                    """
                    DELETE FROM aria_recent_context
                    WHERE created_at < NOW() - INTERVAL 14 DAY
                    """
                )

    async def recent_context(
        self,
        *,
        user_id: int | None,
        guild_id: int | None,
        limit: int = 4,
    ) -> list[dict]:
        if not db.pool or (user_id is None and guild_id is None):
            return []

        clauses = []
        params = []
        if guild_id is not None:
            clauses.append("guild_id = %s")
            params.append(guild_id)
        if user_id is not None:
            clauses.append("(user_id = %s OR user_id IS NULL)")
            params.append(user_id)

        where = " AND ".join(clauses)
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT source_kind, prompt_text, reply_text, created_at
                    FROM aria_recent_context
                    WHERE {where}
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (*params, int(limit)),
                )
                rows = await cur.fetchall() or []

        out = []
        for row in reversed(rows):
            if isinstance(row, dict):
                out.append(row)
            else:
                out.append(
                    {
                        "source_kind": row[0],
                        "prompt_text": row[1],
                        "reply_text": row[2],
                        "created_at": row[3],
                    }
                )
        return out

    async def store_file_artifact(
        self,
        *,
        user_id: int,
        guild_id: int | None,
        source_kind: str,
        filename: str | None,
        language_hint: str | None,
        original_code: str,
        current_code: str,
        offer_pending: bool,
    ) -> None:
        if not db.pool or not user_id or not original_code or not current_code:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_file_artifacts (
                        user_id, guild_id, source_kind, filename, language_hint,
                        original_code, current_code, offer_pending
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        guild_id,
                        (source_kind or "code_review")[:32],
                        (filename or "")[:255] or None,
                        (language_hint or "")[:32] or None,
                        original_code,
                        current_code,
                        bool(offer_pending),
                    ),
                )
                await cur.execute(
                    """
                    DELETE FROM aria_file_artifacts
                    WHERE updated_at < NOW() - INTERVAL 14 DAY
                    """
                )

    async def latest_file_artifact(
        self,
        *,
        user_id: int,
        guild_id: int | None,
        require_pending: bool = False,
    ) -> dict | None:
        if not db.pool or not user_id:
            return None
        clauses = ["user_id = %s"]
        params: list[object] = [user_id]
        if guild_id is not None:
            clauses.append("(guild_id = %s OR guild_id IS NULL)")
            params.append(guild_id)
        if require_pending:
            clauses.append("offer_pending = TRUE")
        where = " AND ".join(clauses)
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT id, source_kind, filename, language_hint, original_code, current_code, offer_pending, updated_at
                    FROM aria_file_artifacts
                    WHERE {where}
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    tuple(params),
                )
                row = await cur.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            return row
        cols = ["id", "source_kind", "filename", "language_hint", "original_code", "current_code", "offer_pending", "updated_at"]
        return dict(zip(cols, row, strict=False))

    async def consume_file_offer(self, *, artifact_id: int) -> None:
        if not db.pool or not artifact_id:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE aria_file_artifacts
                    SET offer_pending = FALSE
                    WHERE id = %s
                    """,
                    (artifact_id,),
                )

    async def record_command_pattern(
        self,
        *,
        action_name: str,
        phrase: str,
        user_id: int | None,
        guild_id: int | None,
        outcome: str = "observed",
    ) -> None:
        if not db.pool:
            return
        normalized_phrase = normalize_phrase(phrase)
        if not normalized_phrase or not action_name:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_command_learning (
                        action_name, normalized_phrase, outcome,
                        first_seen_user_id, last_seen_user_id, last_seen_guild_id, hit_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, 1)
                    ON DUPLICATE KEY UPDATE
                        outcome = VALUES(outcome),
                        last_seen_user_id = VALUES(last_seen_user_id),
                        last_seen_guild_id = VALUES(last_seen_guild_id),
                        hit_count = hit_count + 1
                    """,
                    (action_name[:64], normalized_phrase, outcome[:32], user_id, user_id, guild_id),
                )

    async def remember_repair_outcome(
        self,
        *,
        symptom_signature: str,
        repair_action: str,
        repair_scope: str = "swarm",
        success: bool,
        last_error: str | None = None,
        meta_json: str | None = None,
    ) -> None:
        if not db.pool or not symptom_signature or not repair_action:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_repair_memory (
                        symptom_signature, repair_action, repair_scope,
                        success_count, failure_count, last_outcome, last_error, meta_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        success_count = success_count + VALUES(success_count),
                        failure_count = failure_count + VALUES(failure_count),
                        last_outcome = VALUES(last_outcome),
                        last_error = VALUES(last_error),
                        meta_json = COALESCE(VALUES(meta_json), meta_json)
                    """,
                    (
                        symptom_signature[:255],
                        repair_action[:128],
                        repair_scope[:64],
                        1 if success else 0,
                        0 if success else 1,
                        "success" if success else "failure",
                        (last_error or "")[:4000] or None,
                        meta_json,
                    ),
                )

    async def sample_terms(self, *, limit: int = 12) -> list[str]:
        if not db.pool:
            return list(DEFAULT_LEARNED_TERMS[:limit])
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT term FROM aria_learned_terms
                    ORDER BY weight DESC, updated_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
        return [row[0] for row in rows] if rows else list(DEFAULT_LEARNED_TERMS[:limit])

    async def top_command_patterns(self, *, limit: int = 6) -> list[tuple[str, str, str, int]]:
        if not db.pool:
            return []
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT action_name, normalized_phrase, outcome, hit_count
                    FROM aria_command_learning
                    ORDER BY hit_count DESC, updated_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
        return [(row[0], row[1], row[2], row[3]) for row in rows]

    async def recent_repair_patterns(self, *, limit: int = 5) -> list[str]:
        if not db.pool:
            return []
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT symptom_signature, repair_action, success_count, failure_count
                    FROM aria_repair_memory
                    ORDER BY success_count DESC, updated_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
        return [f"{sig} -> {action} (S:{succ}/F:{fail})" for sig, action, succ, fail in rows]

    async def recent_conversation_patterns(self, *, limit: int = 8) -> list[tuple[str, str, str | None, int]]:
        if not db.pool:
            return []
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT normalized_input, normalized_output, response_style, hit_count
                    FROM aria_conversation_patterns
                    WHERE pattern_type = 'conversation'
                    ORDER BY hit_count DESC, updated_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
        return [(row[0], row[1], row[2], row[3]) for row in rows]

    async def similar_conversation_patterns(self, prompt: str, *, limit: int = 3) -> list[str]:
        normalized_prompt = normalize_phrase(prompt)
        if not normalized_prompt:
            return []
        candidates = await self.recent_conversation_patterns(limit=20)
        scored = []
        for inp, out, style, hit_count in candidates:
            score = SequenceMatcher(None, normalized_prompt, inp).ratio()
            if score < 0.48:
                continue
            snippet = f"input='{inp[:120]}' -> reply style={style or 'unknown'} output='{(out or '')[:120]}' (match={score:.2f}, hits={hit_count})"
            scored.append((score, hit_count, snippet))
        scored.sort(reverse=True)
        return [item[2] for item in scored[:limit]]

    async def similar_command_patterns(self, phrase: str, *, limit: int = 4) -> list[str]:
        normalized_phrase = normalize_phrase(phrase)
        if not normalized_phrase:
            return []
        candidates = await self.top_command_patterns(limit=30)
        scored = []
        for action_name, learned_phrase, outcome, hit_count in candidates:
            score = SequenceMatcher(None, normalized_phrase, learned_phrase).ratio()
            if score < 0.42:
                continue
            snippet = f"{action_name}: '{learned_phrase[:120]}' outcome={outcome} hits={hit_count} match={score:.2f}"
            scored.append((score, hit_count, snippet))
        scored.sort(reverse=True)
        return [item[2] for item in scored[:limit]]

    async def action_success_hints(self, *, symptom_signature: str | None = None, repair_scope: str | None = None, limit: int = 6) -> list[str]:
        if not db.pool:
            return []
        clauses = []
        params = []
        if symptom_signature:
            clauses.append("symptom_signature = %s")
            params.append(symptom_signature[:255])
        if repair_scope:
            clauses.append("repair_scope = %s")
            params.append(repair_scope[:64])
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT symptom_signature, repair_action, repair_scope, success_count, failure_count, last_outcome
                    FROM aria_repair_memory
                    {where}
                    ORDER BY (success_count - failure_count) DESC, success_count DESC, updated_at DESC
                    LIMIT %s
                    """,
                    (*params, limit),
                )
                rows = await cur.fetchall()
        return [
            f"{sig} [{scope}] -> {action} outcome={outcome} S:{succ}/F:{fail}"
            for sig, action, scope, succ, fail, outcome in (rows or [])
        ]

    async def top_command_families(self, *, limit: int = 8) -> list[str]:
        if not db.pool:
            return []
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT action_name, COUNT(*) AS variants, SUM(hit_count) AS total_hits
                    FROM aria_command_learning
                    GROUP BY action_name
                    ORDER BY total_hits DESC, variants DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
        return [f"{action} ({variants} phrase variants, {hits} total hits)" for action, variants, hits in (rows or [])]

    async def build_prompt_fragment(
        self,
        *,
        prompt: str | None = None,
        command_phrase: str | None = None,
        user_id: int | None = None,
        guild_id: int | None = None,
    ) -> str:
        learned_terms = await self.sample_terms(limit=10)
        command_patterns = await self.top_command_patterns(limit=5)
        repair_patterns = await self.recent_repair_patterns(limit=4)
        command_families = await self.top_command_families(limit=6)
        similar_conversations = await self.similar_conversation_patterns(prompt or "", limit=3)
        similar_commands = await self.similar_command_patterns(command_phrase or prompt or "", limit=4)
        scoped_repair_hints = await self.action_success_hints(symptom_signature=normalize_phrase(prompt or "")[:255] if prompt else None, limit=3)
        recent_context = await self.recent_context(user_id=user_id, guild_id=guild_id, limit=4)
        parts = []
        if recent_context:
            snippets = []
            for entry in recent_context:
                source = str(entry.get("source_kind") or "chat").replace("_", " ")
                user_text = compact_excerpt(entry.get("prompt_text") or "", limit=140)
                reply_text = compact_excerpt(entry.get("reply_text") or "", limit=140)
                if reply_text:
                    snippets.append(f"[{source}] user='{user_text}' | aria='{reply_text}'")
                else:
                    snippets.append(f"[{source}] user='{user_text}'")
            parts.append("Recent same-user context to connect follow-up questions: " + "; ".join(snippets) + ".")
        if learned_terms:
            parts.append("Aria's learned vocabulary bank: " + ", ".join(learned_terms) + ".")
        if command_patterns:
            pattern_text = "; ".join(
                f"{action}: '{phrase[:80]}' ({outcome}, hits={hits})"
                for action, phrase, outcome, hits in command_patterns
            )
            parts.append("Observed user command phrasing that often maps cleanly to swarm control: " + pattern_text + ".")
        if similar_commands:
            parts.append("Command phrasing most similar to the current request: " + "; ".join(similar_commands) + ".")
        if similar_conversations:
            parts.append("Past conversation patterns similar to this prompt: " + "; ".join(similar_conversations) + ".")
        if repair_patterns:
            parts.append("Recent successful repair patterns: " + "; ".join(repair_patterns) + ".")
        if scoped_repair_hints:
            parts.append("Repair actions that worked for similar symptoms: " + "; ".join(scoped_repair_hints) + ".")
        if command_families:
            parts.append("Command families Aria sees most often: " + "; ".join(command_families) + ".")
        return "\n".join(parts)

    async def craft_insult_seed(self, target_name: str, prompt: str) -> str:
        prompt_terms = extract_candidate_terms(prompt, limit=6)
        learned_terms = await self.sample_terms(limit=6)
        insult = build_dynamic_insult(target_name, prompt_terms + learned_terms)
        await self.record_generated_insult(insult, target_name=target_name, source_terms=prompt_terms + learned_terms)
        return insult
