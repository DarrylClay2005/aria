from __future__ import annotations

import pytest

from core.learning import LearningEngine


@pytest.mark.asyncio
async def test_learning_engine_prompt_fragment_works_without_database_pool():
    fragment = await LearningEngine().build_prompt_fragment(prompt="recover stalled playback")

    assert "learned vocabulary bank" in fragment.lower()


@pytest.mark.asyncio
async def test_learning_engine_insult_seed_works_without_database_pool():
    insult = await LearningEngine().craft_insult_seed("Sonata", "this automation is catastrophically broken")

    assert "Sonata" in insult
    assert insult.endswith(".")
