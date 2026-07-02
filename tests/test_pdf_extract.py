from pdf_extract import find_deviation_number, find_effective_date, safe_filename


def test_effective_date_long_form():
    assert find_effective_date("Effective Date: October 15, 2025\n...") == "October 15, 2025"


def test_effective_date_inline():
    assert find_effective_date("This deviation is effective on March 3, 2026.") == "March 3, 2026"


def test_effective_date_iso_and_slash():
    assert find_effective_date("effective 2025-10-15") == "2025-10-15"
    assert find_effective_date("effective: 10/15/2025") == "10/15/2025"


def test_effective_date_absent():
    assert find_effective_date("no dates here") is None


def test_deviation_number_with_prefix():
    assert find_deviation_number("Class Deviation CD-2025-04 is issued") == "CD-2025-04"


def test_deviation_number_numeric():
    assert find_deviation_number("Class Deviation 25-01 supersedes...") == "25-01"


def test_deviation_number_fallback():
    assert find_deviation_number("nothing to match", "GSA_dev.pdf") == "GSA_dev.pdf"
    assert find_deviation_number("nothing to match") is None


def test_safe_filename_deterministic_and_sanitized():
    url = "https://www.acquisition.gov/sites/default/files/page_file_uploads/GSA%20Dev (1).pdf"
    a, b = safe_filename(url), safe_filename(url)
    assert a == b
    hash_part, name_part = a.split("_", 1)
    assert len(hash_part) == 16
    assert " " not in name_part and "(" not in name_part
