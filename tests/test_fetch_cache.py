import asyncio

import fetch


def test_cache_roundtrip():
    url = "https://example.com/cached-page"
    fetch.cache_path(url).write_text("<html>cached body</html>")
    # client=None proves the network is never touched on a cache hit
    got_url, html, err = asyncio.run(fetch.fetch_one(url, None, use_cache=True))
    assert got_url == url
    assert html == "<html>cached body</html>"
    assert err is None


def test_cache_ignored_by_default():
    url = "https://example.com/cached-page-2"
    fetch.cache_path(url).write_text("<html>stale</html>")
    # With use_cache=False the fetch goes to the (null) client and errors,
    # instead of returning the cached copy.
    _, html, err = asyncio.run(fetch.fetch_one(url, None, use_cache=False))
    assert html is None
    assert err
