import asyncio
import json
import sys
from asyncio import Semaphore, TaskGroup
from datetime import UTC, datetime, timedelta
from typing import Any

from httpx import AsyncClient
from typer import Typer

URL = "https://datahub.creodias.eu/odata/v1/Products"
CONCURRENCY = 5
TOP = 1000

app = Typer()

L2A_PREFIXES = [
    "S2A_MSIL2A",
    "S2B_MSIL2A",
    "S2C_MSIL2A",
]


def format_datetime(d: datetime) -> str:
    return d.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


async def _process_hour(
    client: AsyncClient,
    semaphore: Semaphore,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]] | tuple[datetime, datetime]:
    filter_parts = [
        "Collection/Name eq 'SENTINEL-2'",
        f"ContentDate/Start gt {format_datetime(start)}",
        f"ContentDate/Start lt {format_datetime(end)}",
        "("
        + " or ".join(f"startswith(Name,'{prefix}')" for prefix in L2A_PREFIXES)
        + ")",
        "contains(Name, 'N0510')",
    ]
    filter = " and ".join(filter_parts)
    records = []
    next_link = None
    try:
        while True:
            async with semaphore:
                if next_link:
                    response = await client.get(next_link)
                else:
                    response = await client.get(
                        URL,
                        params={
                            "$filter": filter,
                            "$top": TOP,
                        },
                    )

            response.raise_for_status()
            data = response.json()
            next_link = data.get("@odata.nextLink")
            records.extend(data["value"])

            if not next_link:
                break
    except Exception as e:
        print(
            f"Failed {start.isoformat()} to {end.isoformat()}: {e}",
            file=sys.stderr,
        )
        return start, end

    return records


async def _run(start_date: datetime, end_date: datetime) -> None:
    semaphore = Semaphore(CONCURRENCY)
    tasks = []
    async with AsyncClient() as client, TaskGroup() as task_group:
        current = start_date
        while current < end_date:
            next_hour = current + timedelta(hours=1)
            tasks.append(
                task_group.create_task(
                    _process_hour(client, semaphore, current, next_hour)
                )
            )
            current = next_hour

    for task in tasks:
        result = await task
        if isinstance(result, tuple):
            start, end = result
            print(
                json.dumps({"failed": [start.isoformat(), end.isoformat()]}),
                file=sys.stderr,
            )
        else:
            print(json.dumps(result))


@app.command()
def run(start_date: datetime, end_date: datetime) -> None:
    """Run the backfill."""
    asyncio.run(_run(start_date.replace(tzinfo=UTC), end_date.replace(tzinfo=UTC)))


if __name__ == "__main__":
    app()
