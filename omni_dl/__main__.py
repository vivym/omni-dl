import time
import json
import logging
from urllib.parse import urlparse
from typing import Annotated

import typer
from rich import print

from .download import OmniDL
from .settings import settings
from .utils.logger import setup_logger

logger = logging.getLogger(__name__)

app = typer.Typer(no_args_is_help=True)
omni_dl = None


def get_omni_dl() -> OmniDL:
    global omni_dl
    if omni_dl is None:
        omni_dl = OmniDL(num_workers=settings.num_workers)
    return omni_dl


@app.command()
def start(
    num_workers: Annotated[
        int,
        typer.Option(..., "-w", "--num-workers", help="Number of download workers"),
    ] = settings.num_workers,
    refresh_interval: Annotated[
        int,
        typer.Option(
            ...,
            "-r",
            "--refresh-interval",
            help="Refresh interval (seconds)",
        ),
    ] = settings.refresh_interval,
) -> None:
    omni_dl = get_omni_dl()
    omni_dl.start(num_workers)

    try:
        while True:
            if not omni_dl.is_running.value:
                break

            global_stats = omni_dl.global_stats
            local_stats = omni_dl.local_stats

            print(
                f"[bold green]Local Stats[/]: "
                f"total_items={local_stats.total_items}, "
                f"total_bytes={local_stats.total_bytes / 1024 / 1024:.02f} MB, "
                f"failed_items={local_stats.failed_items}, "
                f"items_per_s={local_stats.items_per_second:.02f}, "
                f"bytes_per_s={local_stats.bytes_per_second / 1024 / 1024:.02f} MB/s,",
            )

            time.sleep(refresh_interval)
    finally:
        omni_dl.stop()


@app.command()
def create(
    url: Annotated[str, typer.Argument(..., help="URL to download")],
    ext: Annotated[str, typer.Argument(..., help="File extension")] = None,
    headers: Annotated[
        str,
        typer.Option(
            ...,
            "-H",
            "--header",
            help="Headers to use for this download task (JSON format)",
        ),
    ] = None,
    remark: Annotated[
        str,
        typer.Option(
            ...,
            "-r",
            "--remark",
            help="Remark for this download task",
        ),
    ] = None,
) -> None:
    if ext is None:
        path = urlparse(url).path
        file_name = path.split("/")[-1]
        if len(file_name) == 0 or "." not in file_name:
            raise ValueError("Cannot infer extension from url.")
        ext = file_name.split(".")[-1].split("@")[0]

    if headers is not None:
        try:
            headers = json.loads(headers)
        except json.JSONDecodeError:
            typer.echo("Invalid JSON format for headers", err=True)
            raise typer.Exit(1)

    omni_dl = get_omni_dl()
    task = omni_dl.create_task(url, ext, headers, remark)
    if task:
        print(f"[bold green]OK:[/] Created task {task.id}.")
    else:
        print("[bold red]ERROR:[/] Task already exists.")


@app.callback()
def main(verbose: bool = typer.Option(False, "-v", "--verbose")) -> None:
    if verbose:
        settings.log_level = "DEBUG"

    setup_logger()


if __name__ == "__main__":
    app()
