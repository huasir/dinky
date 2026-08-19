"""Dinky MCP Server — phase-1 OpenAPI tools."""

from __future__ import annotations

import json
import time
from typing import Any

from mcp.server.fastmcp import FastMCP

from dinky_mcp.client import DinkyAPIError, DinkyClient

mcp = FastMCP(
    "dinky",
    instructions=(
        "Dinky Flink SQL platform integration. "
        "Requires DINKY_BASE_URL and DINKY_TOKEN environment variables. "
        "Use explain_sql / get_stream_graph with raw FlinkSQL statements, "
        "or submit/cancel/monitor existing tasks by task_id."
    ),
)

_client: DinkyClient | None = None


def _get_client() -> DinkyClient:
    global _client
    if _client is None:
        _client = DinkyClient()
    return _client


def _json_result(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


@mcp.tool()
def dinky_explain_sql(
    statement: str,
    parallelism: int = 1,
    fragment: bool = False,
    statement_set: bool = False,
    batch_model: bool = False,
    run_mode: str = "local",
    task_id: int | None = None,
) -> str:
    """Explain FlinkSQL without executing it.

    Args:
        statement: FlinkSQL to validate/plan.
        parallelism: Default parallelism.
        fragment: Enable SQL fragment variables.
        statement_set: Wrap statements in a statement set.
        batch_model: Run in batch mode.
        run_mode: Flink run mode (local, yarn-session, yarn-per-job, standalone, kubernetes-session).
        task_id: Optional existing Dinky task ID to attach context.
    """
    client = _get_client()
    body = client.build_task_body(
        statement,
        task_id=task_id,
        parallelism=parallelism,
        fragment=fragment,
        statement_set=statement_set,
        batch_model=batch_model,
        run_mode=run_mode,
    )
    try:
        return _json_result(client.explain_sql(body))
    except DinkyAPIError as exc:
        return _json_result({"error": str(exc), "payload": exc.payload})


@mcp.tool()
def dinky_submit_task(
    task_id: int,
    save_point_path: str | None = None,
    variables: dict[str, str] | None = None,
    is_online: bool | None = None,
) -> str:
    """Submit an existing Dinky task by ID.

    Args:
        task_id: Dinky task ID (from catalogue).
        save_point_path: Optional savepoint path for restore.
        variables: Optional global-variable overrides.
        is_online: Online mode flag (only one online job per task).
    """
    client = _get_client()
    try:
        return _json_result(
            client.submit_task(
                task_id,
                save_point_path=save_point_path,
                variables=variables,
                is_online=is_online,
            )
        )
    except DinkyAPIError as exc:
        return _json_result({"error": str(exc), "payload": exc.payload})


@mcp.tool()
def dinky_cancel_task(
    task_id: int,
    with_save_point: bool = False,
    force_cancel: bool = True,
) -> str:
    """Cancel a running Flink job for the given Dinky task.

    Args:
        task_id: Dinky task ID.
        with_save_point: Trigger savepoint before cancel.
        force_cancel: Force cancel if graceful stop fails.
    """
    client = _get_client()
    try:
        return _json_result(
            client.cancel_task(
                task_id,
                with_save_point=with_save_point,
                force_cancel=force_cancel,
            )
        )
    except DinkyAPIError as exc:
        return _json_result({"error": str(exc), "payload": exc.payload})


@mcp.tool()
def dinky_get_job_status(task_id: int) -> str:
    """Get the latest job instance for a Dinky task.

    Args:
        task_id: Dinky task ID.
    """
    client = _get_client()
    try:
        return _json_result(client.get_job_instance_by_task_id(task_id))
    except DinkyAPIError as exc:
        return _json_result({"error": str(exc), "payload": exc.payload})


@mcp.tool()
def dinky_get_stream_graph(
    statement: str,
    parallelism: int = 1,
    fragment: bool = False,
    statement_set: bool = False,
    batch_model: bool = False,
    run_mode: str = "local",
    task_id: int | None = None,
) -> str:
    """Get Flink stream graph (DAG) for FlinkSQL.

    Args:
        statement: FlinkSQL to analyze.
        parallelism: Default parallelism.
        fragment: Enable SQL fragment variables.
        statement_set: Wrap statements in a statement set.
        batch_model: Run in batch mode.
        run_mode: Flink run mode.
        task_id: Optional existing Dinky task ID.
    """
    client = _get_client()
    body = client.build_task_body(
        statement,
        task_id=task_id,
        parallelism=parallelism,
        fragment=fragment,
        statement_set=statement_set,
        batch_model=batch_model,
        run_mode=run_mode,
    )
    try:
        return _json_result(client.get_stream_graph(body))
    except DinkyAPIError as exc:
        return _json_result({"error": str(exc), "payload": exc.payload})


@mcp.tool()
def dinky_get_lineage(task_id: int) -> str:
    """Get data lineage for a Dinky task.

    Args:
        task_id: Dinky task ID.
    """
    client = _get_client()
    try:
        return _json_result(client.get_task_lineage(task_id))
    except DinkyAPIError as exc:
        return _json_result({"error": str(exc), "payload": exc.payload})


@mcp.tool()
def dinky_trigger_savepoint(task_id: int, savepoint_type: str = "trigger") -> str:
    """Trigger savepoint for a running Dinky task.

    Args:
        task_id: Dinky task ID.
        savepoint_type: One of trigger, stop, cancel, canceljob.
    """
    client = _get_client()
    try:
        return _json_result(client.trigger_savepoint(task_id, savepoint_type))
    except DinkyAPIError as exc:
        return _json_result({"error": str(exc), "payload": exc.payload})


@mcp.tool()
def dinky_wait_for_job(
    task_id: int,
    timeout_seconds: int = 300,
    poll_interval_seconds: int = 5,
    target_statuses: list[str] | None = None,
) -> str:
    """Poll job status until terminal state or timeout.

    Args:
        task_id: Dinky task ID.
        timeout_seconds: Max wait time.
        poll_interval_seconds: Seconds between polls.
        target_statuses: Stop when status matches one of these (default: FINISHED, FAILED, CANCELED).
    """
    if target_statuses is None:
        target_statuses = ["FINISHED", "FAILED", "CANCELED", "UNKNOWN"]

    client = _get_client()
    deadline = time.monotonic() + timeout_seconds
    last_response: Any = None

    while time.monotonic() < deadline:
        try:
            last_response = client.get_job_instance_by_task_id(task_id)
        except DinkyAPIError as exc:
            return _json_result({"error": str(exc), "payload": exc.payload, "timed_out": False})

        data = last_response.get("data") if isinstance(last_response, dict) else None
        status = (data or {}).get("status") if isinstance(data, dict) else None
        if status and status.upper() in {s.upper() for s in target_statuses}:
            return _json_result({"done": True, "status": status, "response": last_response})

        time.sleep(poll_interval_seconds)

    return _json_result(
        {
            "done": False,
            "timed_out": True,
            "last_response": last_response,
        }
    )


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
