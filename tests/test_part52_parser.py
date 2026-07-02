from part52_parser import parse_part52_text

SAMPLE = """\
52.204-7 System for Award Management.
As prescribed in 4.1105(a)(1), insert the following provision:
System for Award Management (JAN 2026)
(a) Definitions. As used in this provision...
(b) The Offeror shall be registered in SAM.
(End of provision)
52.232-25 Prompt Payment.
As prescribed in 32.908(c), insert the following clause:
Prompt Payment (JAN 2026)
(a) Invoice payments will be made in accordance with...
(End of clause)
"""


def test_parses_both_sections():
    clauses = parse_part52_text(SAMPLE)
    assert [c["number"] for c in clauses] == ["52.204-7", "52.232-25"]


def test_titles_and_kinds():
    clauses = {c["number"]: c for c in parse_part52_text(SAMPLE)}
    assert clauses["52.204-7"]["title"] == "System for Award Management"
    assert clauses["52.204-7"]["kind"] == "Provision"
    assert clauses["52.232-25"]["title"] == "Prompt Payment"
    assert clauses["52.232-25"]["kind"] == "Clause"


def test_effective_dates():
    for c in parse_part52_text(SAMPLE):
        assert c["effective_date"] == "JAN 2026"


def test_body_trimmed_at_end_marker():
    clauses = {c["number"]: c for c in parse_part52_text(SAMPLE)}
    assert clauses["52.204-7"]["full_text"].endswith("(End of provision)")
    assert clauses["52.232-25"]["full_text"].endswith("(End of clause)")


def test_dedup_keeps_first_occurrence():
    doubled = SAMPLE + SAMPLE.replace("Definitions", "SECOND COPY")
    clauses = [c for c in parse_part52_text(doubled) if c["number"] == "52.204-7"]
    assert len(clauses) == 1
    assert "Definitions" in clauses[0]["full_text"]


def test_unknown_kind_without_markers():
    text = "52.201-1 Some Heading.\nBody text without prescribe line or end marker.\n"
    (clause,) = parse_part52_text(text)
    assert clause["kind"] == "Unknown"
    assert clause["effective_date"] is None
