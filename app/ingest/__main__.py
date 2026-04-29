from __future__ import annotations

import typer

from app.ingest.pipeline import IngestionPipeline
from app.settings import get_settings

cli = typer.Typer(help="Ingest CricSheet data into the local cricket expert knowledge base.")


@cli.command()
def update() -> None:
    settings = get_settings()
    pipeline = IngestionPipeline(settings)
    result = pipeline.update()
    typer.echo(
        f"Update complete: seen={result['seen']} indexed={result['indexed']} "
        f"skipped={result['skipped']} failed={result['failed']}"
    )


@cli.command()
def rebuild() -> None:
    settings = get_settings()
    pipeline = IngestionPipeline(settings)
    result = pipeline.rebuild()
    typer.echo(
        f"Rebuild complete: seen={result['seen']} indexed={result['indexed']} "
        f"skipped={result['skipped']} failed={result['failed']}"
    )


if __name__ == "__main__":
    cli()

