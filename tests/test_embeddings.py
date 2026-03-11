from types import SimpleNamespace

import pytest

from src import embeddings


def _reset_embedding_runtime() -> None:
    embeddings._model = None
    embeddings._model_config = None
    embeddings._client = None
    embeddings._client_config = None
    embeddings._embedding_dim = None


def test_openai_compatible_backend_selected_when_base_url_is_present(monkeypatch):
    monkeypatch.delenv("EMBEDDING_BACKEND", raising=False)
    monkeypatch.setenv("EMBEDDING_BASE_URL", "http://host.docker.internal:1234/v1")

    assert embeddings._get_backend() == "openai-compatible"


def test_encode_batch_with_lmstudio_backend_serializes_embeddings(monkeypatch):
    _reset_embedding_runtime()
    monkeypatch.setenv("EMBEDDING_BACKEND", "lmstudio")
    monkeypatch.setenv("EMBEDDING_BASE_URL", "http://host.docker.internal:1234/v1")
    monkeypatch.setenv("EMBEDDING_MODEL", "text-embedding-nomic-embed-text-v1.5")

    calls = []

    class FakeClient:
        def __init__(self):
            self.embeddings = self

        def create(self, model, input):
            calls.append((model, input))
            return SimpleNamespace(
                data=[
                    SimpleNamespace(index=index, embedding=[float(index + 1), 0.5])
                    for index, _ in enumerate(input)
                ]
            )

    monkeypatch.setattr(embeddings, "_get_openai_client", lambda: FakeClient())

    blobs = embeddings.encode_batch(["first", "second"])

    assert calls == [("text-embedding-nomic-embed-text-v1.5", ["first", "second"])]
    assert len(blobs) == 2
    assert embeddings.get_embedding_dim() == 2
    assert embeddings.decode_embedding(blobs[0]) == pytest.approx([1.0, 0.5])
    assert embeddings.decode_embedding(blobs[1]) == pytest.approx([2.0, 0.5])
