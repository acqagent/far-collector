import importlib
from pathlib import Path


def test_env_overrides(monkeypatch, tmp_path):
    import config
    try:
        monkeypatch.setenv("FAR_DATA_DIR", str(tmp_path / "d"))
        monkeypatch.setenv("FAR_CORPUS_PDF_DIR", str(tmp_path / "corpus"))
        monkeypatch.setenv("FAR_LLM_BASE_URL", "http://elsewhere:9000/v1")
        importlib.reload(config)
        assert config.DATA_DIR == tmp_path / "d"
        assert config.PDF_DIR == tmp_path / "d" / "pdfs"
        assert config.DB_PATH == tmp_path / "d" / "collector.duckdb"
        assert config.CORPUS_PDF_DIR == tmp_path / "corpus"
        assert config.LLM_BASE_URL == "http://elsewhere:9000/v1"
    finally:
        monkeypatch.undo()
        importlib.reload(config)


def test_corpus_paths_default_to_none(monkeypatch):
    import config
    try:
        monkeypatch.delenv("FAR_CORPUS_PDF_DIR", raising=False)
        monkeypatch.delenv("FAR_CORPUS_MANIFEST", raising=False)
        importlib.reload(config)
        assert config.CORPUS_PDF_DIR is None
        assert config.CORPUS_MANIFEST is None
        assert isinstance(config.DATA_DIR, Path)
    finally:
        monkeypatch.undo()
        importlib.reload(config)
