import json
from datetime import timedelta
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.param_functions import Form
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from odmantic import AIOEngine
from motor.motor_asyncio import AsyncIOMotorClient

from omni_dl.settings import settings
from omni_dl.models.user import UserModel
from omni_dl.schemas.user import Token, User
from omni_dl.models.task import TaskModel
from omni_dl.schemas.task import TaskResponse, TaskCreation
from omni_dl.utils.logger import setup_logger
from omni_dl.utils.security import create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
engine: AIOEngine | None = None


@app.on_event("startup")
async def startup_event():
    setup_logger()

    global engine

    mongo_client = AsyncIOMotorClient(settings.mongo_uri)
    engine = AIOEngine(
        client=mongo_client,
        database=settings.mongo_db,
    )
    await engine.configure_database([UserModel, TaskModel])


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/register", response_model=User)
async def register(
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
):
    user = await UserModel.create_user(engine, username, password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User already exists",
        )
    return user


@app.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
):
    user = await UserModel.authenticate(engine, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")


async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)],
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    user = await UserModel.get_user_from_token(engine, token)
    if not user:
        raise credentials_exception
    else:
        return user


async def get_current_active_user(
    current_user: Annotated[User, Depends(get_current_user)],
):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


async def get_current_supper_user(
    current_user: Annotated[User, Depends(get_current_user)],
):
    if not current_user.superuser:
        raise HTTPException(status_code=400, detail="Not supper user")
    return current_user


@app.get("/activate/{username}")
async def activate_user(
    current_user: Annotated[User, Depends(get_current_supper_user)],
    username: str,
):
    user = await UserModel.activate_user(engine, username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User does not exist",
        )
    return user


@app.post("/task", response_model=TaskResponse)
async def create_task(
    current_user: Annotated[User, Depends(get_current_active_user)],
    task: TaskCreation,
):
    created_task = await TaskModel.create_task(
        engine=engine,
        task=task,
        username=current_user.username,
    )
    return {"task": created_task}


@app.get("/task", response_model=TaskResponse)
async def get_pending_task(
    current_user: Annotated[User, Depends(get_current_active_user)],
):
    task = await TaskModel.get_pending_task(engine=engine, username=current_user.username)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_204_NO_CONTENT,
            detail="No pending task",
        )
    return {"task": task}


@app.post("/task/ack")
async def ack_task(
    current_user: Annotated[User, Depends(get_current_active_user)],
    task_id: Annotated[str, Form()],
    download_id: Annotated[str, Form()],
    storage_uri: Annotated[str, Form()],
    storage_options: Annotated[str, Form()],
    size: Annotated[int, Form()],
    checksum: Annotated[str, Form()],
    checksum_algo: Annotated[str, Form()],
):
    try:
        storage_options_obj = json.loads(storage_options)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid storage options",
        )

    await TaskModel.ack_task(
        engine=engine,
        task_id=task_id,
        download_id=download_id,
        storage_uri=storage_uri,
        storage_options=storage_options_obj,
        size=size,
        checksum=checksum,
        checksum_algo=checksum_algo,
        username=current_user.username,
    )
    return {"status": "ok"}


@app.post("/task/retry")
async def retry_task(
    current_user: Annotated[User, Depends(get_current_active_user)],
    task_id: Annotated[str, Form()],
    download_id: Annotated[str, Form()],
):
    await TaskModel.retry_task(
        engine=engine,
        task_id=task_id,
        download_id=download_id,
        username=current_user.username,
    )
    return {"status": "ok"}


@app.post("/task/requeue")
async def retry_task(
    current_user: Annotated[User, Depends(get_current_active_user)],
    task_id: Annotated[str, Form()],
    download_id: Annotated[str, Form()],
):
    await TaskModel.requeue_task(
        engine=engine,
        task_id=task_id,
        download_id=download_id,
        username=current_user.username,
    )
    return {"status": "ok"}


@app.get("/task/stats")
async def get_task_stats(
    current_user: Annotated[User, Depends(get_current_active_user)],
):
    stats = await TaskModel.get_stats(engine=engine)
    return {"stats": stats}
