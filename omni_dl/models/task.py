import uuid
from datetime import datetime, timedelta

from odmantic import AIOEngine, Field, Model, ObjectId
from odmantic.exceptions import DuplicateKeyError
from pymongo import ReturnDocument

from omni_dl.schemas.task import Task, TaskCreation

RETRY_DELAYS = [
    timedelta(seconds=10),
    timedelta(seconds=30),
    timedelta(minutes=1),
    timedelta(minutes=5),
    timedelta(minutes=10),
    timedelta(minutes=30),
    timedelta(hours=1),
]


class TaskModel(Model):
    url: str = Field(index=True, unique=True)
    ext: str
    headers: dict[str, str] | None = None
    remark: str | None = None

    storage_uri: str | None = None
    storage_options: dict[str, str] | None = None
    size: int | None = None
    checksum: str | None = Field(None, index=True)
    checksum_algo: str | None = None

    status: str = Field(default="pending", index=True)
    download_id: str | None = None
    retry_count: int = 0
    to_download_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    created_by: str
    updated_by: str

    def to_task_schema(self) -> Task:
        return Task(
            id=str(self.id),
            url=self.url,
            ext=self.ext,
            headers=self.headers,
            remark=self.remark,
            storage_uri=self.storage_uri,
            storage_options=self.storage_options,
            size=self.size,
            checksum=self.checksum,
            checksum_algo=self.checksum_algo,
            status=self.status,
            download_id=self.download_id,
            retry_count=self.retry_count,
            created_at=self.created_at,
            updated_at=self.updated_at,
            created_by=self.created_by,
            updated_by=self.updated_by,
        )

    @classmethod
    def from_task_creation_schema(cls, task: TaskCreation, username: str) -> "TaskModel":
        return cls(
            url=task.url,
            ext=task.ext,
            headers=task.headers,
            remark=task.remark,
            status="pending",
            created_by=username,
            updated_by=username,
        )

    @classmethod
    async def create_task(cls, engine: AIOEngine, task: TaskCreation, username: str) -> Task | None:
        try:
            task_instance = cls.from_task_creation_schema(task, username)
            saved_task = await engine.save(task_instance)
            return saved_task.to_task_schema()
        except DuplicateKeyError:
            return None

    @classmethod
    async def get_pending_task(cls, engine: AIOEngine, username: str) -> Task:
        collection = engine.get_collection(cls)

        download_id = uuid.uuid5(uuid.NAMESPACE_URL, f"omni-dl/{username}/{uuid.uuid4()}").hex

        doc = await collection.find_one_and_update(
            filter={"status": "pending", "to_download_at": {"$lte": datetime.utcnow()}},
            update={
                "$set": {
                    "status": "processing",
                    "download_id": download_id,
                    "updated_at": datetime.utcnow(),
                    "updated_by": username,
                },
            },
            sort=[("to_download_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if doc is None:
            return None
        else:
            return cls.parse_doc(doc).to_task_schema()

    @classmethod
    async def ack_task(
        cls,
        engine: AIOEngine,
        task_id: str,
        download_id: str,
        storage_uri: str,
        storage_options: dict[str, str],
        size: int,
        checksum: str,
        checksum_algo: str,
        username: str,
    ) -> Task | None:
        async with engine.transaction() as transaction:
            task = await engine.find_one(cls, cls.id == ObjectId(task_id))

            if task is not None and task.download_id == download_id:
                task.storage_uri = storage_uri
                task.storage_options = storage_options
                task.size = size
                task.checksum = checksum
                task.checksum_algo = checksum_algo
                task.status = "completed"
                task.updated_at = datetime.utcnow()
                task.updated_by = username

                await engine.save(task)

            await transaction.commit()

        return task.to_task_schema() if task is not None else None

    @classmethod
    async def retry_task(
        cls,
        engine: AIOEngine,
        task_id: str,
        download_id: str,
        username: str,
    ) -> Task | None:
        async with engine.transaction() as transaction:
            task = await engine.find_one(cls, cls.id == ObjectId(task_id))

            if task is not None and task.download_id == download_id:
                if task.retry_count >= 5:
                    task.status = "failed"
                else:
                    task.status = "pending"
                    now = datetime.utcnow()
                    task.to_download_at = now + RETRY_DELAYS[task.retry_count]

                task.updated_at = now
                task.retry_count += 1
                task.updated_by = username

                await engine.save(task)

            await transaction.commit()

        return task.to_task_schema() if task is not None else None

    @classmethod
    async def requeue_task(
        cls,
        engine: AIOEngine,
        task_id: str,
        download_id: str,
        username: str,
    ) -> Task | None:
        async with engine.transaction() as transaction:
            task = await engine.find_one(cls, cls.id == ObjectId(task_id))

            if task is not None and task.download_id == download_id:
                task.status = "pending"
                now = datetime.utcnow()
                task.to_download_at = now
                task.updated_at = now
                task.updated_by = username

                await engine.save(task)

            await transaction.commit()

        return task.to_task_schema() if task is not None else None

    @classmethod
    async def get_stats(cls, engine: AIOEngine) -> dict[str, int]:
        collection = engine.get_collection(cls)

        pipeline = [
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            {"$project": {"_id": 0, "status": "$_id", "count": 1}},
        ]
        docs = await collection.aggregate(pipeline).to_list(length=None)
        return {doc["status"]: doc["count"] for doc in docs}

    @classmethod
    async def clear_expired_processing_tasks(cls, engine: AIOEngine) -> int:
        collection = engine.get_collection(cls)

        now = datetime.utcnow()
        result = await collection.update_many(
            {"status": "processing", "updated_at": {"$lte": now - timedelta(minutes=30)}},
            {"$set": {
                "status": "pending",
                "download_id": None,
                "updated_at": now,
                "updated_by": "system",
            }},
        )
        return result.modified_count
