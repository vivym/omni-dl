import logging

from celery import Celery

from omni_dl.settings import settings
from omni_dl.models.task import TaskModel
from omni_dl.utils.logger import setup_logger

logger = logging.getLogger(__name__)

app = Celery(
    settings.celery_app_name,
    broker=settings.celery_broker_uri,
    backend=settings.celery_backend_uri,
    broker_connection_retry_on_startup=True,
)
engine = None


def get_engine():
    global engine

    if engine is None:
        from odmantic import AIOEngine
        from motor.motor_asyncio import AsyncIOMotorClient

        mongo_client = AsyncIOMotorClient(settings.mongo_uri)
        engine = AIOEngine(
            client=mongo_client,
            database=settings.mongo_db,
        )

    return engine


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    setup_logger("celery")

    logger.info("Setting up periodic tasks.")

    sender.add_periodic_task(
        5 * 60,
        clear_expired_processing_tasks,
        name="clear expired processing tasks every minute",
    )


@app.task
async def clear_expired_processing_tasks():
    engine = get_engine()
    count = await TaskModel.clear_expired_processing_tasks(engine)
    logger.info(f"Removed {count} expired processing tasks.")
