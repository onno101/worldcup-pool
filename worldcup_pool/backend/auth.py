from __future__ import annotations

import dataclasses

import jwt
from fastapi import HTTPException, Request


@dataclasses.dataclass
class UserContext:
    user_id: str
    email: str | None
    sub: str | None


def get_user_context(request: Request) -> UserContext:  # FastAPI injects Request
    """Identify user from Databricks Apps forwarded access token (JWT payload)."""
    token = request.headers.get("x-forwarded-access-token")
    if not token:
        raise HTTPException(status_code=401, detail="Missing x-forwarded-access-token")

    try:
        payload = jwt.decode(
            token,
            options={
                "verify_signature": False,
                "verify_aud": False,
                "verify_exp": False,
            },
        )
    except jwt.DecodeError:
        raise HTTPException(status_code=401, detail="Invalid access token")

    sub = payload.get("sub")
    email = payload.get("email") or payload.get("upn")
    user_id = (email or sub or "").strip().lower()
    if not user_id:
        raise HTTPException(status_code=401, detail="No user identity in token")

    return UserContext(user_id=user_id, email=email, sub=sub)


def is_admin(user: UserContext) -> bool:
    from worldcup_pool.backend.config import get_settings

    raw = (get_settings().admin_emails or "").strip()
    if raw:
        admins = {e.strip().lower() for e in raw.split(",") if e.strip()}
    else:
        # No default admin — deployer must set ADMIN_EMAILS env var.
        admins = set()
    if not admins:
        return False
    if user.email and user.email.lower() in admins:
        return True
    return user.user_id.lower() in admins
