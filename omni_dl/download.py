import os
import hashlib
import logging
import tempfile
import time
import queue
import json
import signal
import shutil
import multiprocessing as mp
from bisect import bisect_left
from datetime import datetime, timedelta
from functools import wraps, partial

import fsspec
from requests import Session
from requests.exceptions import RequestException

from .settings import settings
from .schemas.task import (
    Task, TaskResponse, TaskDownloadResult, TaskLocalStats, TaskGlobalStats, TaskCreation
)
from .schemas.user import Token
from .utils.logger import setup_logger

logger = logging.getLogger(__name__)

LOGIN_URL = f"{settings.api_url}/token"
GET_TASK_URL = f"{settings.api_url}/task"
ACK_TASK_URL = f"{settings.api_url}/task/ack"
RETRY_TASK_URL = f"{settings.api_url}/task/retry"
REQUEUE_TASK_URL = f"{settings.api_url}/task/requeue"
SHARED_FS = None
SAVE_RETRY_INTERVALS = [1, 2, 5, 10, 30]


def get_storage_options() -> dict[str, str]:
    if settings.storage_options is None:
        return {}
    return settings.storage_options


def get_fs() -> fsspec.AbstractFileSystem:
    global SHARED_FS

    if SHARED_FS is None:
        SHARED_FS = fsspec.filesystem(
            settings.storage_uri.split(":")[0],
            **get_storage_options(),
        )

    return SHARED_FS


def save_item(local_file_name: str, checksum: str, ext: str) -> str | None:
    fs = get_fs()
    storage_uri = os.path.join(settings.storage_uri, f"{checksum}.{ext}")

    for i in range(5):
        try:
            with open(local_file_name, "rb") as lf, fs.open(storage_uri, "wb") as rf:
                shutil.copyfileobj(lf, rf)
            return storage_uri
        except Exception as e:
            logger.error(f"Failed to save item {local_file_name}: {e}")
            if i < 4:
                retry_interval = SAVE_RETRY_INTERVALS[i]
                logger.warning(f"Retrying save_item {i+1}/5 in {retry_interval} seconds...")
                time.sleep(retry_interval)

    return None


def login(sess: Session) -> bool:
    try:
        rsp = sess.post(
            LOGIN_URL,
            data={
                "username": settings.username,
                "password": settings.password,
            },
        )
        if rsp.status_code == 401:
            logger.error(f"Failed to login: {rsp.text}")
            return False
        elif rsp.status_code == 200:
            token = Token.parse_obj(rsp.json())
            sess.headers.update({"Authorization": f"Bearer {token.access_token}"})
            return True
        else:
            logger.error(f"Failed to login: {rsp.status_code} {rsp.text}")
            return False
    except Exception as e:
        logger.error(f"Failed to login: {e}")
        return False


class UnauthorizedException(Exception):
    ...


def auto_login(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        for i in range(2):
            try:
                return fn(*args, **kwargs)
            except UnauthorizedException:
                if i == 0:
                    login(args[0])

    return wrapper


@auto_login
def get_task(sess: Session) -> Task | None:
    try:
        rsp = sess.get(GET_TASK_URL)
        if rsp.status_code == 204:
            return None
        elif rsp.status_code == 200:
            return TaskResponse.parse_obj(rsp.json()).task
        elif rsp.status_code == 401:
            raise UnauthorizedException()
        else:
            logger.error(f"Failed to get task: {rsp.status_code} {rsp.text}")
            return None
    except RequestException as e:
        logger.error(f"Failed to get task: {e}")
        return None


@auto_login
def ack_task(sess: Session, task: Task) -> bool:
    try:
        rsp = sess.post(
            ACK_TASK_URL,
            data={
                "task_id": task.id,
                "download_id": task.download_id,
                "storage_uri": task.storage_uri,
                "storage_options": json.dumps(task.storage_options),
                "size": task.size,
                "checksum": task.checksum,
                "checksum_algo": task.checksum_algo,
            },
        )
        if rsp.status_code == 401:
            raise UnauthorizedException()

        rsp.raise_for_status()

        return True
    except RequestException as e:
        logger.error(f"Failed to ack task {task.id}: {e}")
        return False


@auto_login
def retry_task(sess: Session, task: Task) -> bool:
    try:
        rsp = sess.post(
            RETRY_TASK_URL,
            data={
                "task_id": task.id,
                "download_id": task.download_id,
            },
        )
        if rsp.status_code == 401:
            raise UnauthorizedException()

        rsp.raise_for_status()

        return True
    except RequestException as e:
        logger.error(f"Failed to retry task {task.id}: {e}")
        return False


@auto_login
def requeue_task(sess: Session, task: Task) -> bool:
    try:
        rsp = sess.post(
            REQUEUE_TASK_URL,
            data={
                "task_id": task.id,
                "download_id": task.download_id,
            },
        )
        if rsp.status_code == 401:
            raise UnauthorizedException()

        rsp.raise_for_status()

        return True
    except RequestException as e:
        logger.error(f"Failed to requeue task {task.id}: {e}")
        return False


@auto_login
def create_task(sess: Session, task: TaskCreation) -> Task | None:
    try:
        rsp = sess.post(
            GET_TASK_URL,
            json=task.dict(),
        )
        if rsp.status_code == 401:
            raise UnauthorizedException()

        rsp.raise_for_status()

        task = TaskResponse.parse_obj(rsp.json()).task

        return task
    except RequestException as e:
        logger.error(f"Failed to create task {task.id}: {e}")
        return None


def download_worker(
    index: int,
    task_queue: queue.Queue[Task],
    result_queue: queue.Queue[TaskDownloadResult],
    byte_speed_queue: queue.Queue[datetime, int],
) -> None:
    setup_logger(
        process_type="worker",
        process_index=index,
    )

    with Session() as sess:
        while True:
            task = task_queue.get()
            if task is None:
                break

            try:
                rsp = sess.get(task.url, headers=task.headers, stream=True)
                rsp.raise_for_status()

                hasher = hashlib.sha256()

                with tempfile.NamedTemporaryFile(prefix="omni-dl-") as f:
                    size = 0
                    for chunk in rsp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            hasher.update(chunk)
                            size += len(chunk)
                            byte_speed_queue.put((datetime.utcnow(), len(chunk)))

                    f.flush()

                    task.size = size
                    task.checksum = hasher.hexdigest()
                    task.checksum_algo = "sha256"

                    storage_uri = save_item(f.name, task.checksum, task.ext)
                    if storage_uri is None:
                        result_queue.put(
                            TaskDownloadResult(
                                type="save_error",
                                task=task,
                            )
                        )
                        break
                    task.storage_uri = storage_uri
                    task.storage_options = get_storage_options()

                result_queue.put(
                    TaskDownloadResult(
                        type="success",
                        task=task,
                    )
                )
            except RequestException as e:
                result_queue.put(
                    TaskDownloadResult(
                        type="download_error",
                        error=str(e),
                        task=task,
                    )
                )
            except Exception as e:
                result_queue.put(
                    TaskDownloadResult(
                        type="error",
                        error=str(e),
                        task=task,
                    )
                )
                break


def master(
    task_queue: queue.Queue[Task],
    result_queue: queue.Queue[TaskDownloadResult],
    item_speed_queue: queue.Queue[datetime, int],
    num_workers: int,
    is_running,
    is_closing,
    is_all_workers_stopped,
    total_items,
    total_bytes,
    failed_items,
):
    setup_logger(process_type="master")

    is_running.value = True

    sess = Session()

    def request_task():
        if not is_running.value or is_closing.value:
            return

        logger.debug(f"Requesting task...")
        task = get_task(sess)
        logger.debug(f"Got task: {task}")
        if task:
            task_queue.put(task)

    for _ in range(num_workers * 2):
        request_task()

    while True:
        try:
            result = result_queue.get(timeout=1.0)
        except queue.Empty:
            if is_all_workers_stopped.value:
                break
            else:
                if task_queue.qsize() < num_workers:
                    request_task()

                continue

        typ, task, error = result.type, result.task, result.error
        request_task()

        if typ == "success":
            logger.debug(f"Downloaded {task.url} to {task.storage_uri}. Acking...")
            if not ack_task(sess, result.task):
                logger.error(f"Failed to ack {task.url}. Shutting down...")
                is_running.value = False
            else:
                item_speed_queue.put((datetime.utcnow(), 1))
                total_items.value += 1
                total_bytes.value += task.size
                logger.debug(f"Successfully acked {task.url}.")
        elif typ == "download_error":
            logger.warning(f"Failed to download {task.url}. Error: {error}. Retrying...")
            failed_items.value += 1
            if not retry_task(sess, result.task):
                logger.error(f"Failed to retry {task.url}. Shutting down...")
                is_running.value = False
            else:
                logger.debug(f"Successfully retried {task.url}.")
        elif typ == "save_error":
            logger.error(f"Failed to save {task.url}. Shutting down...")
            requeue_task(sess, result.task)
            is_running.value = False
        elif typ == "error":
            logger.error(f"Failed to process {task.url}. Error: {error}. Shutting down...")
            requeue_task(sess, result.task)
            is_running.value = False
        else:
            requeue_task(sess, result.task)
            logger.error(f"Unknown result type {typ}. Shutting down...")
            is_running.value = False


class OmniDL:
    def __init__(self, num_workers: int) -> None:
        self.num_workers = num_workers

        self.task_queue: queue.Queue[Task] | None = None
        self.result_queue: queue.Queue[TaskDownloadResult] | None = None
        self.byte_speed_queue: queue.Queue[tuple[datetime, int]] | None = None
        self.item_speed_queue: queue.Queue[tuple[datetime, int]] | None = None

        self.byte_speed_list: list[tuple[datetime, int]] = []
        self.item_speed_list: list[tuple[datetime, int]] = []

        self.workers: list[mp.Process] | None = None
        self.master: mp.Process | None = None

        self.sess = Session()

        self.is_running = mp.Value("b", True)
        self.is_closing = mp.Value("b", False)
        self.is_all_workers_stopped = mp.Value("b", False)
        self.total_items = mp.Value("i", 0)
        self.total_bytes = mp.Value("i", 0)
        self.failed_items = mp.Value("i", 0)

        self._global_stats_update_time = datetime.utcnow()
        self._global_stats: TaskGlobalStats | None = None

        self.manager = mp.Manager()

    def start(self, num_workers: int | None = None) -> None:
        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

        if num_workers is not None:
            self.num_workers = num_workers

        self.task_queue = self.manager.Queue()
        self.result_queue = self.manager.Queue()
        self.byte_speed_queue = self.manager.Queue()
        self.item_speed_queue = self.manager.Queue()

        self.workers = [
            mp.Process(
                target=partial(download_worker, i),
                args=(self.task_queue, self.result_queue, self.byte_speed_queue),
            )
            for i in range(self.num_workers)
        ]

        for w in self.workers:
            w.start()

        self.master = mp.Process(
            target=master,
            args=(
                self.task_queue,
                self.result_queue,
                self.item_speed_queue,
                self.num_workers,
                self.is_running,
                self.is_closing,
                self.is_all_workers_stopped,
                self.total_items,
                self.total_bytes,
                self.failed_items,
            ),
        )
        self.master.start()

        signal.signal(signal.SIGINT, original_sigint_handler)

    def stop(self) -> None:
        logger.info("Force stopping...")

        self.is_closing.value = True
        for _ in range(self.num_workers):
            self.task_queue.put(None)

        for w in self.workers:
            w.join()

        logger.info("All workers stopped.")

        self.is_all_workers_stopped.value = True
        self.master.join()

        logger.info("Master stopped.")

    @property
    def local_stats(self) -> TaskLocalStats:
        queue_size = self.item_speed_queue.qsize()
        for _ in range(queue_size):
            self.item_speed_list.append(self.item_speed_queue.get())

        queue_size = self.byte_speed_queue.qsize()
        for _ in range(queue_size):
            self.byte_speed_list.append(self.byte_speed_queue.get())

        # remove old items
        if len(self.item_speed_list) > 0:
            index = bisect_left(self.item_speed_list, (datetime.utcnow() - timedelta(minutes=1), 0))
            self.item_speed_list = self.item_speed_list[index:]

        if len(self.byte_speed_list) > 0:
            index = bisect_left(self.byte_speed_list, (datetime.utcnow() - timedelta(minutes=1), 0))
            self.byte_speed_list = self.byte_speed_list[index:]

        items_per_second = sum([i[1] for i in self.item_speed_list]) / 60.0
        bytes_per_second = sum([i[1] for i in self.byte_speed_list]) / 60.0

        return TaskLocalStats(
            items_per_second=items_per_second,
            bytes_per_second=bytes_per_second,
            total_items=self.total_items.value,
            total_bytes=self.total_bytes.value,
            failed_items=self.failed_items.value,
        )

    @property
    def global_stats(self) -> TaskGlobalStats:
        if self._global_stats is None or (
            datetime.utcnow() - self._global_stats_update_time
        ).total_seconds() > 1.0:
            self._global_stats = TaskGlobalStats(
                pending=0,
                downloading=0,
                downloaded=0,
                failed=0,
                total=0,
            )
            self._global_stats_update_time = datetime.utcnow()

        return self._global_stats

    def create_task(
        self,
        url: str,
        ext: str,
        headers: dict[str, str] | None = None,
        remark: str | None = None,
    ) -> Task | None:
        task = TaskCreation(
            url=url,
            ext=ext,
            headers=headers,
            remark=remark,
        )
        return create_task(self.sess, task)
