from datetime import datetime

from odmantic import AIOEngine, Model, Field
from passlib.context import CryptContext

from omni_dl.schemas.user import User
from omni_dl.utils.security import decode_access_token

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class UserModel(Model):
    username: str = Field(index=True, unique=True)
    hashed_password: str
    disabled: bool = True
    superuser: bool = False
    created_at: datetime

    def to_user_schema(self) -> User:
        return User(
            username=self.username,
            disabled=self.disabled,
            superuser=self.superuser,
            created_at=self.created_at,
        )

    @classmethod
    async def authenticate(
        cls, engine: AIOEngine, username: str, password: str
    ) -> User | None:
        user = await engine.find_one(cls, cls.username == username)
        if not user:
            return None

        if not pwd_context.verify(password, user.hashed_password):
            return None

        return user.to_user_schema()

    @classmethod
    async def get_user_from_token(cls, engine: AIOEngine, token: str) -> User | None:
        token_data = decode_access_token(token)
        if token_data is None:
            return None

        user = await engine.find_one(cls, cls.username == token_data.username)
        if not user:
            return None
        else:
            return user.to_user_schema()

    @classmethod
    async def create_user(
        cls, engine: AIOEngine, username: str, password: str
    ) -> User | None:
        user = await engine.find_one(cls, cls.username == username)
        if user:
            return None

        hashed_password = pwd_context.hash(password)
        user = cls(
            username=username,
            hashed_password=hashed_password,
            disabled=True,
            superuser=False,
            created_at=datetime.utcnow(),
        )
        await engine.save(user)
        return user.to_user_schema()

    @classmethod
    async def activate_user(cls, engine: AIOEngine, username: str) -> bool:
        user = await engine.find_one(cls, cls.username == username)
        if user is None:
            return False
        else:
            user.disabled = False
            await engine.save(user)
            return True
