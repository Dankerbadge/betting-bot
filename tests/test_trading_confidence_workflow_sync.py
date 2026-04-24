from __future__ import annotations

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
MAKEFILE_PATH = ROOT / "Makefile"
TRADING_CONFIDENCE_WORKFLOW_PATH = (
    ROOT / ".github" / "workflows" / "trading-confidence.yml"
)

TRADING_CONFIDENCE_REQUIRED_TRIGGER_PATHS = {
    "betbot/**",
    "infra/digitalocean/**",
    "Makefile",
    ".github/workflows/trading-confidence.yml",
}
TRADING_CONFIDENCE_MAKEFILE_TEST_VARS = (
    "ALPHA_CORE_TESTS",
    "PROFIT_READINESS_TESTS",
    "GATECHAIN_SMOKE_TESTS",
)


def _read_required_text(path: Path) -> str:
    assert (
        path.exists()
    ), f"Expected file is missing: {path}. Create it and keep it in sync with Makefile test lists and workflow triggers."
    return path.read_text(encoding="utf-8")


def _strip_optional_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _strip_inline_comment(value: str) -> str:
    in_single = False
    in_double = False
    escaped = False

    for idx, char in enumerate(value):
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            continue
        if (
            char == "#"
            and not in_single
            and not in_double
            and (idx == 0 or value[idx - 1].isspace())
        ):
            return value[:idx].rstrip()

    return value.rstrip()


def _parse_makefile_test_list(makefile_text: str, variable_name: str) -> list[str]:
    lines = makefile_text.splitlines()
    assign_pattern = re.compile(rf"^\s*{re.escape(variable_name)}\s*:?=\s*(.*)$")

    for start_index, line in enumerate(lines):
        match = assign_pattern.match(line)
        if not match:
            continue

        segments: list[str] = []
        current_index = start_index
        current_segment = match.group(1)

        while True:
            segment_without_comment = _strip_inline_comment(current_segment).strip()
            is_continued = segment_without_comment.endswith("\\")

            if is_continued:
                segment_without_comment = segment_without_comment[:-1].rstrip()

            if segment_without_comment:
                segments.append(segment_without_comment)

            if not is_continued:
                break

            current_index += 1
            assert (
                current_index < len(lines)
            ), f"Makefile {variable_name} ends with '\\' continuation but has no following line."
            current_segment = lines[current_index]

        parsed_tests = [token for token in " ".join(segments).split() if token]
        assert (
            parsed_tests
        ), f"Parsed Makefile {variable_name} is empty; add test paths so trading-confidence workflow triggers can stay in sync."
        return parsed_tests

    raise AssertionError(
        f"Could not find {variable_name} := ... assignment in Makefile. Add {variable_name} with test paths and sync it with .github/workflows/trading-confidence.yml."
    )


def _parse_required_makefile_tests(makefile_text: str) -> dict[str, list[str]]:
    return {
        variable_name: _parse_makefile_test_list(makefile_text, variable_name)
        for variable_name in TRADING_CONFIDENCE_MAKEFILE_TEST_VARS
    }


def _line_indent(raw_line: str) -> int:
    return len(raw_line) - len(raw_line.lstrip(" "))


def _find_top_level_key(lines: list[str], key: str) -> tuple[int, int]:
    target = f"{key}:"
    for index, raw_line in enumerate(lines):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if _line_indent(raw_line) == 0 and stripped == target:
            return index, 0

    raise AssertionError(
        f"Workflow is missing top-level '{target}' section in {TRADING_CONFIDENCE_WORKFLOW_PATH}."
    )


def _find_child_key(
    lines: list[str],
    *,
    parent_index: int,
    parent_indent: int,
    key: str,
    context: str,
) -> tuple[int, int]:
    target = f"{key}:"

    index = parent_index + 1
    while index < len(lines):
        raw_line = lines[index]
        stripped = raw_line.strip()

        if not stripped or stripped.startswith("#"):
            index += 1
            continue

        indent = _line_indent(raw_line)
        if indent <= parent_indent:
            break

        if stripped == target:
            return index, indent

        index += 1

    raise AssertionError(
        f"Workflow is missing '{target}' under '{context}' in {TRADING_CONFIDENCE_WORKFLOW_PATH}."
    )


def _parse_yaml_dash_list(
    lines: list[str],
    *,
    list_key_index: int,
    list_key_indent: int,
    context: str,
) -> list[str]:
    values: list[str] = []
    index = list_key_index + 1

    while index < len(lines):
        raw_line = lines[index]
        stripped = raw_line.strip()

        if not stripped or stripped.startswith("#"):
            index += 1
            continue

        indent = _line_indent(raw_line)
        if indent <= list_key_indent:
            break

        if stripped.startswith("- "):
            raw_value = stripped[2:].strip()
            value = _strip_optional_quotes(_strip_inline_comment(raw_value).strip())
            if value:
                values.append(value)

        index += 1

    assert (
        values
    ), f"No entries found under '{context}' in {TRADING_CONFIDENCE_WORKFLOW_PATH}."
    return values


def _extract_workflow_paths(workflow_text: str, trigger: str) -> list[str]:
    lines = workflow_text.splitlines()

    on_index, on_indent = _find_top_level_key(lines, "on")
    trigger_index, trigger_indent = _find_child_key(
        lines,
        parent_index=on_index,
        parent_indent=on_indent,
        key=trigger,
        context="on",
    )
    paths_index, paths_indent = _find_child_key(
        lines,
        parent_index=trigger_index,
        parent_indent=trigger_indent,
        key="paths",
        context=f"on.{trigger}",
    )

    return _parse_yaml_dash_list(
        lines,
        list_key_index=paths_index,
        list_key_indent=paths_indent,
        context=f"on.{trigger}.paths",
    )


def _workflow_contains_run_command(workflow_text: str, expected_command: str) -> bool:
    lines = workflow_text.splitlines()
    index = 0

    while index < len(lines):
        raw_line = lines[index]
        stripped = raw_line.strip()

        if not stripped or stripped.startswith("#") or not stripped.startswith("run:"):
            index += 1
            continue

        run_indent = _line_indent(raw_line)
        run_value = _strip_inline_comment(stripped[len("run:") :].strip())

        if run_value and not run_value.startswith(("|", ">")):
            normalized_run = _strip_optional_quotes(run_value)
            if normalized_run == expected_command:
                return True
            index += 1
            continue

        index += 1
        while index < len(lines):
            block_line = lines[index]
            block_stripped = block_line.strip()

            if not block_stripped:
                index += 1
                continue

            block_indent = _line_indent(block_line)
            if block_indent <= run_indent:
                break

            normalized_block_line = _strip_optional_quotes(
                _strip_inline_comment(block_stripped).strip()
            )
            if normalized_block_line == expected_command:
                return True

            index += 1

    return False


def test_trading_confidence_workflow_invokes_make_target() -> None:
    workflow_text = _read_required_text(TRADING_CONFIDENCE_WORKFLOW_PATH)

    assert _workflow_contains_run_command(
        workflow_text, "make test-trading-confidence"
    ), (
        "Expected .github/workflows/trading-confidence.yml to include a run step: "
        "'make test-trading-confidence'."
    )


def test_trading_confidence_workflow_verifies_clean_tree() -> None:
    workflow_text = _read_required_text(TRADING_CONFIDENCE_WORKFLOW_PATH)

    assert _workflow_contains_run_command(
        workflow_text, "git diff --exit-code"
    ), (
        "Expected .github/workflows/trading-confidence.yml to include a clean-tree "
        "guard: 'git diff --exit-code'."
    )


def test_trading_confidence_workflow_trigger_paths_include_required_entries() -> None:
    workflow_text = _read_required_text(TRADING_CONFIDENCE_WORKFLOW_PATH)
    push_paths = set(_extract_workflow_paths(workflow_text, "push"))
    pull_request_paths = set(_extract_workflow_paths(workflow_text, "pull_request"))

    for trigger_name, trigger_paths in (
        ("push", push_paths),
        ("pull_request", pull_request_paths),
    ):
        missing = sorted(TRADING_CONFIDENCE_REQUIRED_TRIGGER_PATHS - trigger_paths)
        assert not missing, (
            "Missing required entries in on."
            f"{trigger_name}.paths for .github/workflows/trading-confidence.yml: "
            f"{missing}. Add them to prevent workflow drift."
        )


def test_trading_confidence_workflow_paths_cover_required_makefile_test_sets() -> None:
    makefile_text = _read_required_text(MAKEFILE_PATH)
    workflow_text = _read_required_text(TRADING_CONFIDENCE_WORKFLOW_PATH)

    required_tests_by_variable = _parse_required_makefile_tests(makefile_text)
    push_paths = set(_extract_workflow_paths(workflow_text, "push"))
    pull_request_paths = set(_extract_workflow_paths(workflow_text, "pull_request"))

    for variable_name, required_tests in required_tests_by_variable.items():
        missing_in_push = [path for path in required_tests if path not in push_paths]
        missing_in_pull_request = [
            path for path in required_tests if path not in pull_request_paths
        ]

        assert not missing_in_push, (
            f"These {variable_name} entries are missing from on.push.paths in "
            f".github/workflows/trading-confidence.yml: {missing_in_push}. "
            "Sync workflow path filters with Makefile."
        )
        assert not missing_in_pull_request, (
            f"These {variable_name} entries are missing from on.pull_request.paths "
            f"in .github/workflows/trading-confidence.yml: {missing_in_pull_request}. "
            "Sync workflow path filters with Makefile."
        )
