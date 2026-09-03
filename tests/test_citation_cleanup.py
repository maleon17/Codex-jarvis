import pytest

from test_trigger_authorization import codex_ask


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "До \ue200cite\ue202turn0search0\ue202turn0search1\ue201 после",
            "До  после",
        ),
        (
            "Ответ: citeturn0search0turn0search1 готово; turn7news2 navlist",
            "Ответ:  готово;  ",
        ),
        (
            "A \ue200cite\ue202turn0search0\ue201 B citeturn0search7"
            "citeturn0news8navlist C turn3view4turn3finance5 D E",
            "A  B  C  D E",
        ),
        (
            "Легитимный текст: cite, turn the page, navigation list.",
            "Легитимный текст: cite, turn the page, navigation list.",
        ),
    ],
)
def test_strip_inline_citations(source, expected):
    assert codex_ask._strip_inline_citations(source) == expected


def test_strip_inline_citations_removes_orphaned_pua_controls():
    assert codex_ask._strip_inline_citations("До\ue202между\ue20bпосле") == "Домеждупосле"
