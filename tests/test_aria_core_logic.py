from __future__ import annotations

import random
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from core.intent_parser import IntentParser
from core.learning import build_dynamic_insult, extract_candidate_terms
from core.swarm_control import SwarmController


class LearningHelpersTests(unittest.TestCase):
    def test_extract_candidate_terms_filters_common_noise(self) -> None:
        terms = extract_candidate_terms(
            "Aria this catastrophic clownshow automation keeps sabotaging every weird playlist and http://example.com spam"
        )

        self.assertIn("catastrophic", terms)
        self.assertIn("clownshow", terms)
        self.assertIn("automation", terms)
        self.assertNotIn("aria", terms)
        self.assertTrue(all("http" not in term for term in terms))

    def test_build_dynamic_insult_keeps_target_name(self) -> None:
        random.seed(4)
        insult = build_dynamic_insult("Desmond", ["catastrophe", "scrapheap"])

        self.assertIn("Desmond", insult)
        self.assertTrue(insult.endswith("."))


class IntentParserTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.parser = IntentParser()

    async def test_parse_play_command_with_drone(self) -> None:
        result = await self.parser.parse("aria play daft punk around the world via melodic")

        self.assertEqual(result["action"], "play")
        self.assertEqual(result["data"]["drone"], "melodic")
        self.assertEqual(result["data"]["query"], "daft punk around the world")

    async def test_parse_filter_command(self) -> None:
        result = await self.parser.parse("filter nightcore on harmonic")

        self.assertEqual(result["action"], "swarm_filter")
        self.assertEqual(result["data"]["filter_type"], "nightcore")
        self.assertEqual(result["data"]["drone"], "harmonic")

    async def test_parse_set_home_command(self) -> None:
        result = await self.parser.parse("aria home nexus <#123456789012345678>")

        self.assertEqual(result["action"], "swarm_set_home")
        self.assertEqual(result["data"]["drone"], "nexus")
        self.assertEqual(result["data"]["channel_id"], 123456789012345678)

    async def test_non_control_prompt_stays_unknown(self) -> None:
        result = await self.parser.parse("stop insulting me for five seconds")

        self.assertEqual(result["action"], "unknown")


class SwarmControllerTests(unittest.IsolatedAsyncioTestCase):
    async def test_leave_targets_active_drone_before_fallback(self) -> None:
        controller = SwarmController()
        ctx = SimpleNamespace(guild=SimpleNamespace(id=42), guild_id=42)

        with patch.object(controller, "active_drones", AsyncMock(return_value=["nexus"])), patch.object(
            controller,
            "direct",
            AsyncMock(return_value="Injected `LEAVE` directly into `nexus`."),
        ) as direct_mock:
            result = await controller.leave(ctx)

        direct_mock.assert_awaited_once_with(ctx, "nexus", "LEAVE")
        self.assertEqual(result, "Injected `LEAVE` directly into `nexus`.")

    async def test_leave_requires_explicit_node_when_no_target_is_resolved(self) -> None:
        controller = SwarmController()
        ctx = SimpleNamespace(guild=SimpleNamespace(id=42), guild_id=42, author=SimpleNamespace(voice=None))

        with patch.object(controller, "active_drones", AsyncMock(return_value=[])):
            result = await controller.leave(ctx)

        self.assertEqual(result, "I couldn't identify which swarm node should leave. Name the node explicitly.")


if __name__ == "__main__":
    unittest.main()
