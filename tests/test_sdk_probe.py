from copilotd.config import Settings
from copilotd.sdk.probe import SdkProbe, _response_matches


def test_static_sdk_matrix_tracks_released_contract(tmp_path) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)

    matrix = SdkProbe(settings).static_matrix()

    assert matrix["sdk_version"] == "1.0.8"
    assert matrix["event_count"] == 114
    assert matrix["audited_main_event_count"] == 116
    assert matrix["audited_main_only_events"] == [
        "factory.run_updated",
        "session.context_cleared",
    ]
    assert matrix["capabilities"]["pre_registered_on_event"].supported


def test_live_probe_expected_response_requires_exact_assistant_message() -> None:
    events = [
        {
            "type": "assistant.message",
            "data": {"content": "COPILOTD_ACCEPTANCE_AUTH_OK"},
        }
    ]

    assert _response_matches(events, "COPILOTD_ACCEPTANCE_AUTH_OK")
    assert not _response_matches(events, "WRONG_ACCOUNT_SENTINEL")
    assert not _response_matches(
        [{"type": "session.idle", "data": {}}],
        "COPILOTD_ACCEPTANCE_AUTH_OK",
    )
