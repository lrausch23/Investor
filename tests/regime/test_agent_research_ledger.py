from __future__ import annotations

from src.regime.agent_research_ledger import append_trial, verify_trial_ledger


def test_trial_ledger_hash_chain_counts_failures(tmp_path) -> None:
    ledger = tmp_path / "arl_trials.jsonl"

    append_trial(
        ledger,
        {"hypothesis": "quality momentum", "verdict": "killed"},
        data_snapshot_hash="snapshot-a",
    )
    append_trial(
        ledger,
        {"hypothesis": "valuation cap", "verdict": "promising"},
        data_snapshot_hash="snapshot-a",
    )

    status = verify_trial_ledger(ledger)
    assert status.valid is True
    assert status.trial_count == 2


def test_trial_ledger_detects_tampering(tmp_path) -> None:
    ledger = tmp_path / "arl_trials.jsonl"
    append_trial(ledger, {"hypothesis": "h1", "verdict": "killed"}, data_snapshot_hash="snapshot-a")
    append_trial(ledger, {"hypothesis": "h2", "verdict": "killed"}, data_snapshot_hash="snapshot-a")

    lines = ledger.read_text(encoding="utf-8").splitlines()
    lines[0] = lines[0].replace("killed", "promising")
    ledger.write_text("\n".join(lines) + "\n", encoding="utf-8")

    status = verify_trial_ledger(ledger)
    assert status.valid is False
    assert "line_1_record_hash_mismatch" in status.issues
    assert "line_2_previous_hash_mismatch" in status.issues


def test_trial_ledger_detects_silent_reset(tmp_path) -> None:
    ledger = tmp_path / "arl_trials.jsonl"
    append_trial(ledger, {"hypothesis": "h1", "verdict": "killed"}, data_snapshot_hash="snapshot-a")

    ledger.write_text("", encoding="utf-8")

    status = verify_trial_ledger(ledger)
    assert status.valid is False
    assert "state_trial_count_mismatch" in status.issues
