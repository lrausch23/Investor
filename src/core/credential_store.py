from __future__ import annotations

import base64
import datetime as dt
import hashlib
import os
from typing import Optional

try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "cryptography is required for encrypted credential storage. "
        "Install dependencies with: pip install -r requirements.txt. "
        f"Original error: {type(e).__name__}: {e}"
    ) from e
from sqlalchemy.orm import Session

from src.db.models import ExternalCredential
from src.utils.time import utcnow


class CredentialError(Exception):
    pass


def secret_key_available() -> bool:
    v = os.environ.get("APP_SECRET_KEY")
    return bool(v and v.strip())


def _fernet() -> Fernet:
    secret = os.environ.get("APP_SECRET_KEY")
    if not secret or not secret.strip():
        raise CredentialError("APP_SECRET_KEY is required to store credentials in DB.")
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    key = base64.urlsafe_b64encode(digest)
    return Fernet(key)


def encrypt_value(plaintext: str) -> str:
    token = _fernet().encrypt(plaintext.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_value(token: str) -> str:
    try:
        out = _fernet().decrypt(token.encode("utf-8"))
        return out.decode("utf-8")
    except InvalidToken as e:
        raise CredentialError("Failed to decrypt credential (wrong APP_SECRET_KEY?).") from e


def mask_secret(value: Optional[str], *, keep_last: int = 4) -> str:
    if value is None:
        return "—"
    v = str(value)
    if v == "":
        return "—"
    k = max(0, int(keep_last))
    suffix = v[-k:] if k and len(v) >= k else v
    return ("*" * 10) + suffix


def upsert_credential(session: Session, *, connection_id: int, key: str, plaintext: str) -> None:
    if not secret_key_available():
        raise CredentialError("APP_SECRET_KEY is required to store credentials in DB.")
    row = (
        session.query(ExternalCredential)
        .filter(ExternalCredential.connection_id == connection_id, ExternalCredential.key == key)
        .one_or_none()
    )
    token = encrypt_value(plaintext)
    now = utcnow()
    if row is None:
        session.add(
            ExternalCredential(
                connection_id=connection_id,
                key=key,
                value_encrypted=token,
                created_at=now,
                updated_at=now,
            )
        )
    else:
        row.value_encrypted = token
        row.updated_at = now


def get_credential(session: Session, *, connection_id: int, key: str) -> Optional[str]:
    row = (
        session.query(ExternalCredential)
        .filter(ExternalCredential.connection_id == connection_id, ExternalCredential.key == key)
        .one_or_none()
    )
    if row is None:
        return None
    if not secret_key_available():
        # Cannot decrypt without a key; treat as unavailable.
        return None
    return decrypt_value(row.value_encrypted)


def get_credential_masked(session: Session, *, connection_id: int, key: str) -> str:
    row = (
        session.query(ExternalCredential)
        .filter(ExternalCredential.connection_id == connection_id, ExternalCredential.key == key)
        .one_or_none()
    )
    if row is None:
        return "—"
    # Show last 4 of the encrypted token as an opaque “fingerprint” if we can't decrypt.
    if not secret_key_available():
        suffix = row.value_encrypted[-4:] if row.value_encrypted else None
        return mask_secret(suffix)
    plaintext = decrypt_value(row.value_encrypted)
    return mask_secret(plaintext)
