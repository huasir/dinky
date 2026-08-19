"""HTTP client for Dinky OpenAPI."""

from __future__ import annotations

import os
from typing import Any

import httpx


class DinkyAPIError(Exception):
    """Raised when Dinky returns a non-success response."""

    def __init__(self, message: str, *, status_code: int | None = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class DinkyClient:
    """Thin wrapper around Dinky /openapi REST endpoints."""

    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        *,
        timeout: float = 120.0,
    ):
        self.base_url = (base_url or os.environ.get("DINKY_BASE_URL", "http://127.0.0.1:8888")).rstrip("/")
        self.token = token or os.environ.get("DINKY_TOKEN", "")
        if not self.token:
            raise ValueError("DINKY_TOKEN is required. Create one in Dinky: 认证中心 > 令牌")
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {
            "dinky-token": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        with httpx.Client(timeout=self.timeout) as client:
            response = client.request(method, url, headers=self._headers(), params=params, json=json)

        if response.status_code >= 400:
            raise DinkyAPIError(
                f"HTTP {response.status_code}: {response.text[:500]}",
                status_code=response.status_code,
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise DinkyAPIError(f"Invalid JSON response from {url}: {response.text[:200]}") from exc

        if isinstance(body, dict) and body.get("success") is False:
            raise DinkyAPIError(
                body.get("msg") or "Dinky API returned success=false",
                payload=body,
            )

        return body

    def explain_sql(self, task_body: dict[str, Any]) -> Any:
        return self._request("POST", "/openapi/explainSql", json=task_body)

    def submit_task(
        self,
        task_id: int,
        *,
        save_point_path: str | None = None,
        variables: dict[str, str] | None = None,
        is_online: bool | None = None,
    ) -> Any:
        body: dict[str, Any] = {"id": task_id}
        if save_point_path is not None:
            body["savePointPath"] = save_point_path
        if variables is not None:
            body["variables"] = variables
        if is_online is not None:
            body["isOnline"] = is_online
        return self._request("POST", "/openapi/submitTask", json=body)

    def cancel_task(
        self,
        task_id: int,
        *,
        with_save_point: bool = False,
        force_cancel: bool = True,
    ) -> Any:
        return self._request(
            "GET",
            "/openapi/cancel",
            params={
                "id": task_id,
                "withSavePoint": with_save_point,
                "forceCancel": force_cancel,
            },
        )

    def get_job_instance_by_task_id(self, task_id: int) -> Any:
        return self._request("GET", "/openapi/getJobInstanceByTaskId", params={"id": task_id})

    def get_stream_graph(self, task_body: dict[str, Any]) -> Any:
        return self._request("POST", "/openapi/getStreamGraph", json=task_body)

    def get_task_lineage(self, task_id: int) -> Any:
        return self._request("GET", "/openapi/getTaskLineage", params={"id": task_id})

    def trigger_savepoint(self, task_id: int, savepoint_type: str = "trigger") -> Any:
        return self._request(
            "POST",
            "/openapi/savepointTask",
            json={"taskId": task_id, "type": savepoint_type},
        )

    @staticmethod
    def build_task_body(
        statement: str,
        *,
        task_id: int | None = None,
        parallelism: int = 1,
        fragment: bool = False,
        statement_set: bool = False,
        batch_model: bool = False,
        run_mode: str = "local",
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "statement": statement,
            "parallelism": parallelism,
            "fragment": fragment,
            "statementSet": statement_set,
            "batchModel": batch_model,
            "type": run_mode,
        }
        if task_id is not None:
            body["id"] = task_id
        if extra:
            body.update(extra)
        return body
