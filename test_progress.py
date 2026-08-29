import unittest

from codex_ask_watcher import TurnState, _item_label


class ProgressRenderingTests(unittest.TestCase):
    def test_waiting_does_not_publish_synthetic_thought(self):
        state = TurnState("request")
        state.add_notification("turn/started", {})
        self.assertEqual(state.progress(), "")

    def test_tool_progress_uses_concrete_label_and_no_generic_fallback(self):
        label, content = _item_label({
            "type": "command_execution",
            "command": "printf hello",
        })
        self.assertEqual(label, "🔧 Bash")
        self.assertEqual(content, "printf hello")
        self.assertNotIn("Выполняю", label)
        self.assertNotIn("Действие Codex", label)
        self.assertNotIn("выполняется", content)

    def test_agent_delta_is_neutral_writing_progress(self):
        state = TurnState("request")
        state.add_notification("item/agentMessage/delta", {
            "itemId": "agent-1", "delta": "Готово",
        })
        progress = state.progress()
        self.assertIn("✍️ Готово", progress)
        self.assertNotIn("🤔", progress)


if __name__ == "__main__":
    unittest.main()
