from datetime import datetime

from .base import BaseSchema


class Token(BaseSchema):
    access_token: str
    token_type: str


class TokenData(BaseSchema):
    username: str


class User(BaseSchema):
    username: str
    disabled: bool
    superuser: bool
    created_at: datetime
