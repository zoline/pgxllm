"""
pgxllm.core.llm.watsonx
-----------------------
IBM watsonx.ai provider.

Authentication (auto-detected):
  username 없음 → IBM Cloud IAM  : iam.cloud.ibm.com/identity/token
  username 있음 → CP4D on-prem   : {base_url}/icp4d-api/v1/authorize

Required parameters:
  api_key    : IBM Cloud API key  또는 CP4D API key
  project_id : watsonx.ai project ID
  base_url   : regional endpoint (default: https://us-south.ml.cloud.ibm.com)
               CP4D: https://<cpd-host>
  model      : model ID (e.g. ibm/granite-34b-code-instruct)
  username   : CP4D 로그인 사용자명 (CP4D 전용, IBM Cloud 시 생략)
"""
from __future__ import annotations

import json
import logging
import ssl
import time
import urllib.request
import urllib.error
from typing import Optional

from .base import LLMProvider, LLMResponse

log = logging.getLogger(__name__)

IAM_URL             = "https://iam.cloud.ibm.com/identity/token"
WATSONX_API_VERSION = "2024-05-01"


class WatsonXProvider(LLMProvider):
    """
    IBM watsonx.ai text/chat API provider.

    IBM Cloud : IAM bearer token (iam.cloud.ibm.com), 만료 5분 전 자동 갱신.
    CP4D      : CP4D authorize endpoint, 만료 5분 전 자동 갱신.
    """

    def __init__(
        self,
        api_key:    str,
        project_id: str,
        model:      str  = "ibm/granite-34b-code-instruct",
        base_url:   str  = "https://us-south.ml.cloud.ibm.com",
        timeout:    int  = 600,
        username:   str  = "",
        verify_ssl: bool = True,
    ):
        if not api_key:
            raise ValueError("watsonx provider requires api_key")
        if not project_id:
            raise ValueError("watsonx provider requires project_id")

        self._api_key    = api_key
        self._project_id = project_id
        self._model      = model
        self._base_url   = base_url.rstrip("/")
        self._timeout    = timeout
        self._username   = username   # CP4D only; empty = IBM Cloud IAM
        self._ssl_ctx    = None if verify_ssl else self._make_insecure_ctx()

        self._token:     Optional[str] = None
        self._token_exp: float         = 0.0

    @staticmethod
    def _make_insecure_ctx() -> ssl.SSLContext:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        return ctx

    @property
    def model_name(self) -> str:
        return self._model

    def _get_token(self) -> str:
        """Bearer 토큰 반환 (캐시, 만료 5분 전 자동 갱신)."""
        if self._token and time.time() < self._token_exp:
            return self._token
        if self._username:
            return self._get_cpd_token()
        return self._get_iam_token()

    def _get_iam_token(self) -> str:
        """IBM Cloud IAM bearer token."""
        data = (
            "grant_type=urn:ibm:params:oauth:grant-type:apikey"
            f"&apikey={self._api_key}"
        ).encode()
        req = urllib.request.Request(IAM_URL, data=data, headers={
            "Content-Type": "application/x-www-form-urlencoded",
        })
        try:
            with urllib.request.urlopen(req, timeout=30, context=self._ssl_ctx) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"IBM IAM token error {e.code}: {detail}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"IBM IAM connection error: {e}") from e

        self._token     = body["access_token"]
        expires_in      = int(body.get("expires_in", 3600))
        self._token_exp = time.time() + expires_in - 300
        return self._token

    def _get_cpd_token(self) -> str:
        """CP4D on-prem bearer token ({base_url}/icp4d-api/v1/authorize)."""
        url  = f"{self._base_url}/icp4d-api/v1/authorize"
        data = json.dumps({"username": self._username, "api_key": self._api_key}).encode()
        req  = urllib.request.Request(url, data=data, headers={
            "Content-Type": "application/json",
        })
        try:
            with urllib.request.urlopen(req, timeout=30, context=self._ssl_ctx) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"CP4D token error {e.code}: {detail}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"CP4D connection error: {e}") from e

        self._token     = body["token"]
        # CP4D 토큰 만료 시간이 없으면 1시간 가정
        expires_in      = int(body.get("expires_in", 3600))
        self._token_exp = time.time() + expires_in - 300
        return self._token

    def complete(
        self,
        system:      str,
        user:        str,
        *,
        temperature: float = 0.0,
        max_tokens:  int   = 2048,
    ) -> LLMResponse:
        token = self._get_token()

        payload = {
            "model_id":   self._model,
            "project_id": self._project_id,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            "parameters": {
                "max_new_tokens": max_tokens,
                "temperature":    temperature,
            },
        }

        url  = f"{self._base_url}/ml/v1/text/chat?version={WATSONX_API_VERSION}"
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(url, data=data, headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {token}",
        })
        try:
            with urllib.request.urlopen(req, timeout=self._timeout, context=self._ssl_ctx) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"watsonx.ai API error {e.code}: {detail}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"watsonx.ai connection error: {e}") from e

        choice = body["choices"][0]
        text   = choice["message"]["content"]
        usage  = body.get("usage", {})
        return LLMResponse(
            text=text,
            model=self._model,
            input_tokens=usage.get("prompt_tokens", 0),
            output_tokens=usage.get("completion_tokens", 0),
            raw=body,
        )
