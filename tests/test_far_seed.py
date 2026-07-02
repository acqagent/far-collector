from far_seed import agency_from_filename, parse_guide, parts_from_filename

GUIDE_HTML = """
<html><body>
<h3>FAR Part 1 <a href="/far-overhaul/far-part-deviation-guide/far-overhaul-part-1">Part 1</a>
    Issuance Date: May 1, 2025 UPDATE: June 2, 2025</h3>
<h3>FAR Part 52 <a href="/far-overhaul/far-part-deviation-guide/far-overhaul-part-52">Part 52</a></h3>
<a href="/sites/default/files/page_file_uploads/GSA_Class_Deviation_Parts-1-6.pdf?v=2">GSA</a>
<a href="/sites/default/files/page_file_uploads/GSA_Class_Deviation_Parts-1-6.pdf">GSA dup</a>
<a href="/sites/default/files/page_file_uploads/DOD_Deviation_Part-12.pdf">DoD</a>
<a href="https://example.com/not-a-deviation.pdf">offsite</a>
</body></html>
"""


def test_agency_from_filename():
    assert agency_from_filename("GSA_Class_Deviation_Parts-1-6.pdf") == ("GSA", False)
    assert agency_from_filename("USAID_RFO_Deviation_Parts-1-6-10-11.pdf") == ("USAID", False)
    assert agency_from_filename("DOD_Deviation_Part-12.pdf") == ("DoD", True)
    assert agency_from_filename("mystery.pdf")[0] == "Unknown"


def test_parts_from_filename():
    assert parts_from_filename("USAID_RFO_Deviation_Parts-1-6-10-11.pdf") == [1, 6, 10, 11]
    assert parts_from_filename("GSA_Deviation_Part-12.pdf") == [12]
    assert parts_from_filename("no_part_info.pdf") == []


def test_parse_guide_parts():
    parts, _ = parse_guide(GUIDE_HTML)
    assert [p.part_number for p in parts] == [1, 52]
    p1 = parts[0]
    assert p1.overview_url.endswith("/far-overhaul-part-1")
    assert p1.issued == "May 1, 2025"
    assert p1.updated == "June 2, 2025"
    assert parts[1].issued is None


def test_parse_guide_pdfs_deduped_and_flagged():
    _, pdfs = parse_guide(GUIDE_HTML)
    by_name = {p.filename: p for p in pdfs}
    # query string stripped, duplicate URL collapsed, offsite PDF ignored
    assert set(by_name) == {"GSA_Class_Deviation_Parts-1-6.pdf", "DOD_Deviation_Part-12.pdf"}
    gsa = by_name["GSA_Class_Deviation_Parts-1-6.pdf"]
    assert gsa.agency == "GSA"
    assert gsa.part_numbers == [1, 6]
    assert not gsa.is_dod
    assert gsa.pdf_url.startswith("https://www.acquisition.gov/")
    assert by_name["DOD_Deviation_Part-12.pdf"].is_dod
