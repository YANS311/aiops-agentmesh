import json

import httpx
import pytest

from app.agents.llm import (
    MalformedLLMOutputError,
    MockLLMProvider,
    OpenAICompatibleLLMProvider,
    build_llm_provider,
)
from app.config import Settings, get_settings
from app.skills.loader import SkillRegistry


def _load_pool_skill():
    registry = SkillRegistry(get_settings().skills_dir)
    skill = registry.match_for_fault("db_connection_pool_exhausted")
    assert skill is not None
    return skill


def test_build_llm_provider_selects_mock_by_default_and_fails_closed_without_key() -> None:
    mock_settings = Settings(llm_provider="mock")
    assert isinstance(build_llm_provider(mock_settings), MockLLMProvider)

    ds_missing_key = Settings(llm_provider="deepseek", llm_api_key="")
    with pytest.raises(ValueError, match="LLM_API_KEY"):
        build_llm_provider(ds_missing_key)

    ds_valid = Settings(
        llm_provider="deepseek",
        llm_base_url="https://api.deepseek.com/v1",
        llm_model="deepseek-chat",
        llm_api_key="sk-test-placeholder",
    )
    provider = build_llm_provider(ds_valid)
    assert isinstance(provider, OpenAICompatibleLLMProvider)


@pytest.mark.asyncio
async def test_openai_compatible_provider_validates_structured_json() -> None:
    skill = _load_pool_skill()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer sk-test-placeholder"
        payload = json.loads(request.content)
        assert payload["model"] == "deepseek-chat"
        assert payload["response_format"] == {"type": "json_object"}
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "summary": "DeepSeek triage plan",
                                    "log_tool": "query_service_error_logs",
                                    "db_tool": "inspect_pg_stat_activity",
                                }
                            )
                        }
                    }
                ]
            },
        )

    provider = OpenAICompatibleLLMProvider(
        base_url="https://api.deepseek.com/v1",
        api_key="sk-test-placeholder",
        model="deepseek-chat",
        transport=httpx.MockTransport(handler),
    )
    plan = await provider.plan_triage(
        fault_type="db_connection_pool_exhausted",
        service_name="payment-core",
        alert_payload={},
        skill=skill,
    )
    assert plan.summary == "DeepSeek triage plan"
    assert plan.log_tool == "query_service_error_logs"
    assert plan.db_tool == "inspect_pg_stat_activity"


@pytest.mark.asyncio
async def test_openai_compatible_provider_fail_closed_on_malformed_json_and_http_error() -> None:
    skill = _load_pool_skill()

    def bad_json_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "not-json-at-all"}}]},
        )

    bad_provider = OpenAICompatibleLLMProvider(
        base_url="https://api.deepseek.com/v1",
        api_key="sk-test-placeholder",
        model="deepseek-chat",
        transport=httpx.MockTransport(bad_json_handler),
    )
    with pytest.raises(MalformedLLMOutputError):
        await bad_provider.plan_triage(
            fault_type="db_connection_pool_exhausted",
            service_name="payment-core",
            alert_payload={},
            skill=skill,
        )

    def missing_field_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": json.dumps({"tool_name": "only_one"})}}]},
        )

    schema_provider = OpenAICompatibleLLMProvider(
        base_url="https://api.deepseek.com/v1",
        api_key="sk-test-placeholder",
        model="deepseek-chat",
        transport=httpx.MockTransport(missing_field_handler),
    )
    with pytest.raises(MalformedLLMOutputError, match="Pydantic"):
        await schema_provider.plan_diagnostic_step(
            agent_role="LOG_DIAGNOSTICS",
            alert_payload={},
            skill=skill,
        )

    def http_502_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="Bad Gateway")

    err_provider = OpenAICompatibleLLMProvider(
        base_url="https://api.deepseek.com/v1",
        api_key="sk-test-placeholder",
        model="deepseek-chat",
        transport=httpx.MockTransport(http_502_handler),
    )
    with pytest.raises(MalformedLLMOutputError, match="HTTP 502"):
        await err_provider.plan_diagnostic_step(
            agent_role="DB_DIAGNOSTICS",
            alert_payload={},
            skill=skill,
        )
