from rich.align import Align
from rich.layout import Layout
from rich.table import Table
from rich.panel import Panel


def generate_stats_table():
    table = Table(title="Task Stats", expand=True)
    table.add_column("", width=20)
    table.add_column("Count", ratio=1)

    table.add_row("Total", "100")
    table.add_row("Pending", "50")
    table.add_row("Downloading", "30")
    table.add_row("Downloaded", "20")
    table.add_row("Failed", "0")

    return table


def generate_log_panel():
    panel = Panel(
        "This is a log panel.",
        title="Log",
        border_style="magenta",
        padding=(1, 2),
    )

    return panel


def generate_main_layout():
    layout = Layout()

    layout.split_column(
        Layout(
            generate_stats_table(),
            name="stats",
            size=10,
        ),
        Layout(generate_log_panel(), name="log", ratio=3),
    )

    return layout


def generate_ui():
    layout = Layout()

    layout.split_column(
        Layout(
            Panel(
                Align("[b]Omni DL[/b]", align="center"),
                border_style="magenta",
            ),
            name="header",
            size=3,
        ),
        Layout(generate_main_layout(), name="main", ratio=1),
        Layout(
            Panel(
                "Press [b]q[/b] to quit.",
                border_style="magenta",
            ),
            name="footer",
            size=3,
        ),
    )

    return layout
