from __future__ import annotations

import random
import re

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
                        insult_text TEXT NOT NULL,
                        target_name VARCHAR(100) NULL,
                        source_terms TEXT NULL,
                        usage_count INT NOT NULL DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                    """
                )
                for term in DEFAULT_LEARNED_TERMS:
                    await cur.execute(
                        """
                        INSERT INTO aria_learned_terms (term, source_kind, weight)
                        VALUES (%s, 'seed', 3)
                        ON DUPLICATE KEY UPDATE weight = GREATEST(weight, VALUES(weight))
                        """,
                        (term,),
                    )

    async def observe_text(
        self,
        user_id: int | None,
        guild_id: int | None,
        text: str,
        *,
        source_kind: str = "message",
    ) -> list[str]:
        terms = extract_candidate_terms(text)
        if not terms or not db.pool:
            return terms

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for term in terms:
                    await cur.execute(
                        """
                        INSERT INTO aria_learned_terms (
                            term, source_kind, weight, first_seen_user_id, last_seen_user_id, last_seen_guild_id
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

    async def sample_terms(self, *, limit: int = 8) -> list[str]:
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
                    (max(limit * 3, limit),),
                )
                rows = await cur.fetchall()

        terms = [row[0] for row in rows] or list(DEFAULT_LEARNED_TERMS)
        random.shuffle(terms)
        return terms[:limit]

    async def build_prompt_fragment(self, *, limit: int = 8) -> str:
        terms = await self.sample_terms(limit=limit)
        if not terms:
            return ""
        return (
            "Aria's learned vocabulary bank: "
            + ", ".join(f"`{term}`" for term in terms)
            + ". Mix these into fresh insults naturally instead of repeating the same stock lines."
        )

    async def craft_insult_seed(self, target_name: str, prompt: str = "") -> str:
        prompt_terms = extract_candidate_terms(prompt, limit=4)
        learned_terms = await self.sample_terms(limit=6)
        insult = build_dynamic_insult(target_name, prompt_terms + learned_terms)
        await self.record_generated_insult(insult, target_name=target_name, source_terms=prompt_terms + learned_terms)
        return insult

    async def record_generated_insult(
        self,
        insult_text: str,
        *,
        target_name: str | None = None,
        source_terms: list[str] | None = None,
    ) -> None:
        if not insult_text or not db.pool:
            return

        serialized_terms = ",".join(source_terms or [])
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_generated_insults (insult_text, target_name, source_terms)
                    VALUES (%s, %s, %s)
                    """,
                    (insult_text[:500], target_name, serialized_terms[:500]),
                )
