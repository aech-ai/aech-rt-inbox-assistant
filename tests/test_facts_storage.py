import sqlite3
from pathlib import Path

from src.database import get_connection, init_db
from src.facts import ExtractedFact, FactType, FactsExtractor


def _create_legacy_schema(db_path: Path) -> None:
    init_db(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("DROP TRIGGER IF EXISTS facts_ai_fts")
    conn.execute("DROP TRIGGER IF EXISTS facts_ad_fts")
    conn.execute("DROP TRIGGER IF EXISTS facts_au_fts")
    conn.execute("DROP TRIGGER IF EXISTS emails_delete_facts")
    conn.execute("DROP TRIGGER IF EXISTS attachments_delete_facts")
    conn.execute("DROP TABLE IF EXISTS facts_fts")
    conn.execute("DROP TABLE IF EXISTS facts")
    conn.execute(
        """
        CREATE TABLE facts (
            id TEXT PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            fact_type TEXT NOT NULL,
            fact_value TEXT NOT NULL,
            context TEXT,
            confidence REAL DEFAULT 0.8,
            entity_normalized TEXT,
            metadata_json TEXT,
            status TEXT DEFAULT 'active',
            due_date DATETIME,
            extracted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            resolved_at DATETIME,
            FOREIGN KEY(source_id) REFERENCES emails(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO emails (
            id, conversation_id, subject, sender, to_emails, cc_emails,
            received_at, body_preview, updated_at
        )
        VALUES (
            'email-1', 'thread-1', 'Subject', 'sender@example.com',
            '[]', '[]', datetime('now'), 'preview', datetime('now')
        )
        """
    )
    conn.execute(
        """
        INSERT INTO attachments (
            id, email_id, filename, content_type, size_bytes, extraction_status, updated_at
        )
        VALUES (
            'att-1', 'email-1', 'sample.txt', 'text/plain', 5, 'completed', datetime('now')
        )
        """
    )
    conn.execute(
        """
        INSERT INTO facts (
            id, source_type, source_id, fact_type, fact_value, status
        ) VALUES (
            'fact-1', 'email', 'email-1', 'company_name', 'Aech AI', 'active'
        )
        """
    )
    conn.commit()
    conn.close()


def test_init_db_migrates_legacy_facts_table_and_allows_attachment_facts(monkeypatch, tmp_path: Path):
    state_dir = tmp_path / ".inbox-assistant"
    db_path = state_dir / "assistant.sqlite"
    state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("INBOX_STATE_DIR", str(state_dir))
    monkeypatch.setenv("INBOX_DB_PATH", str(db_path))

    _create_legacy_schema(db_path)
    init_db(db_path)

    conn = get_connection(db_path)
    fk_rows = conn.execute("PRAGMA foreign_key_list(facts)").fetchall()
    assert fk_rows == []

    extractor = FactsExtractor()
    stored = extractor.store_facts(
        "attachment",
        "att-1",
        [
            ExtractedFact(
                fact_type=FactType.AMOUNT,
                fact_value="$106.00",
                context="Invoice total due",
            )
        ],
    )
    assert stored == 1

    rows = conn.execute(
        "SELECT source_type, source_id, fact_type, fact_value FROM facts"
    ).fetchall()
    assert len(rows) == 2
    payload = {
        (row["source_type"], row["source_id"], row["fact_type"], row["fact_value"])
        for row in rows
    }
    assert ("email", "email-1", "company_name", "Aech AI") in payload
    assert ("attachment", "att-1", "amount", "$106.00") in payload

    conn.execute("DELETE FROM attachments WHERE id = 'att-1'")
    conn.commit()
    remaining_attachment_facts = conn.execute(
        "SELECT COUNT(*) FROM facts WHERE source_type = 'attachment'"
    ).fetchone()[0]
    conn.close()

    assert remaining_attachment_facts == 0
