from datetime import date

from normalize_dates import normalize


def test_iso():
    assert normalize("2025-10-15") == (date(2025, 10, 15), "iso")


def test_dotted():
    assert normalize("2025.10.15") == (date(2025, 10, 15), "iso")


def test_us_slash():
    assert normalize("10/15/2025") == (date(2025, 10, 15), "iso")
    assert normalize("1/5/2026") == (date(2026, 1, 5), "iso")


def test_long_form():
    assert normalize("October 15, 2025") == (date(2025, 10, 15), "long")


def test_long_form_with_trailing_text():
    d, kind = normalize("October 15, 2025 (Effective immediately)")
    assert d == date(2025, 10, 15)
    assert kind == "long"


def test_immediate():
    assert normalize("Immediately") == (None, "immediate")
    assert normalize("Effective upon issuance") == (None, "immediate")


def test_issuance():
    assert normalize("Date of issuance of the FY26 model language") == (None, "issuance")


def test_delta():
    assert normalize("14 days from signature") == (None, "delta")


def test_empty_and_unparsed():
    assert normalize(None) == (None, "empty")
    assert normalize("   ") == (None, "empty")
    assert normalize("TBD") == (None, "unparsed")
