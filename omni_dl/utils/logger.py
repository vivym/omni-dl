import logging

from omni_dl.settings import settings


def setup_logger(process_type: str | None = None, process_index: int | None = None):
    logger = logging.getLogger("omni_dl")
    logger.setLevel(settings.log_level)

    if process_type is None:
        process_name = "[ main ]"
    elif process_type == "master":
        process_name = "[master]"
    elif process_type == "worker":
        assert isinstance(process_index, int)
        process_name = f"[ w-{process_index:02d} ]"
    elif process_type == "celery":
        process_name = "[celery]"
    else:
        raise ValueError(f"Invalid process_type: {process_type}")

    formatter = logging.Formatter(
        f"%(asctime)s - {process_name} - %(name)s - %(levelname)s - %(message)s"
    )

    ch = logging.StreamHandler()
    ch.setLevel(settings.log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
