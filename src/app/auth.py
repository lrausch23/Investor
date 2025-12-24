from __future__ import annotations

import os
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials


security = HTTPBasic(auto_error=False)


def _expected_password() -> Optional[str]:
    pw = os.environ.get("APP_PASSWORD")
    if pw is not None and pw.strip() == "":
        return None
    return pw


def auth_enabled() -> bool:
    return _expected_password() is not None


def get_actor_from_request(request: Request) -> str:
    return request.headers.get("X-Actor") or os.environ.get("APP_ACTOR_DEFAULT", "local")


def require_actor(credentials: Optional[HTTPBasicCredentials] = Depends(security), request: Request = None) -> str:  # type: ignore[assignment]
    expected = _expected_password()
    if expected is None:
        return get_actor_from_request(request) if request is not None else "local"

    if credentials is None or credentials.password != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username or "user"


def auth_banner_message() -> Optional[str]:
    if auth_enabled():
        return None
    return "WARNING: APP_PASSWORD is not set. This UI is unauthenticated; run locally only."

