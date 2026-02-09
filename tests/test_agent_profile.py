from pathlib import Path
import importlib.util


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "src" / "agent_profile.py"
    spec = importlib.util.spec_from_file_location("inbox_agent_profile_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module at {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_mod = _load_module()
append_agent_profile = _mod.append_agent_profile
load_agent_profile = _mod.load_agent_profile


def test_load_agent_profile_includes_root_and_capability(monkeypatch, tmp_path: Path):
    repo_root = tmp_path / "repo"
    capability_dir = repo_root / "capabilities" / "query_agent"
    capability_dir.mkdir(parents=True)

    (repo_root / "AGENT_PROFILE.md").write_text("root profile", encoding="utf-8")
    (capability_dir / "AGENT_PROFILE.md").write_text("capability profile", encoding="utf-8")

    monkeypatch.setenv("AECH_AGENT_PROFILE_ROOT", str(repo_root))

    profile = load_agent_profile(capability_name="query_agent")

    assert "root profile" in profile
    assert "capability profile" in profile


def test_append_agent_profile_no_file_returns_base(monkeypatch, tmp_path: Path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    monkeypatch.setenv("AECH_AGENT_PROFILE_ROOT", str(repo_root))

    base = "base instructions"
    merged = append_agent_profile(base, capability_name="classification")

    assert merged == base
