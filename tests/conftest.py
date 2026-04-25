# tests/conftest.py
"""
Mock heavy optional dependencies (chromadb, sentence_transformers)
so that pure-function unit tests can run without the full ML stack installed.
"""
import sys
from unittest.mock import MagicMock

# Build a proper package mock so submodule imports like
# `from chromadb.utils import embedding_functions` don't fail.
chromadb_mock = MagicMock()
chromadb_mock.__path__ = []           # marks it as a package
chromadb_mock.__spec__ = MagicMock()

for mod in [
    "chromadb",
    "chromadb.config",
    "chromadb.utils",
    "chromadb.utils.embedding_functions",
    "sentence_transformers",
]:
    if mod not in sys.modules:
        sys.modules[mod] = chromadb_mock
