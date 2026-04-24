from __future__ import annotations

from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
MAKEFILE_PATH = ROOT / "Makefile"
PROFIT_READINESS_WORKFLOW_PATH = ROOT / ".github" / "workflows" / "profit-readiness.yml"

PROFIT_READINESS_REQUIRED_TRIGGER_PATHS = {
    "betbot/**",
    "Makefile",
    ".github/workflows/profit-readiness.yml",
}


def _read_required_text(path: Path) -> str:
    assert (
        path.exists()
    ), f"Expected profit-readiness sync file is missing: {path}. Create .github/workflows/profit-readiness.yml and keep it aligned with Makefile targets."
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
        if char == "#" and not in_single and not in_double and (idx == 0 or value[idx - 1].isspace()):
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
        ), f"Parsed Makefile {variable_name} is empty; add test paths so profit-readiness workflow triggers stay accurate."
        return parsed_tests

    raise AssertionError(
        f"Could not find {variable_name} := ... assignment in Makefile. Add {variable_name} so .github/workflows/profit-readiness.yml can stay in sync."
    )


def _parse_profit_readiness_tests_from_makefile(makefile_text: str) -> list[str]:
    return _parse_makefile_test_list(makefile_text, "PROFIT_READINESS_TESTS")


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
        "Workflow .github/workflows/profit-readiness.yml is missing top-level "
        f"'{target}' section."
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
        "Workflow .github/workflows/profit-readiness.yml is missing "
        f"'{target}' under '{context}'."
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
    ), f"No entries found under '{context}' in .github/workflows/profit-readiness.yml."
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
            if _strip_optional_quotes(run_value) == expected_command:
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

            if _strip_inline_comment(block_stripped) == expected_command:
                return True

            index += 1

    return False


def test_profit_readiness_workflow_invokes_make_target() -> None:
    workflow_text = _read_required_text(PROFIT_READINESS_WORKFLOW_PATH)

    assert _workflow_contains_run_command(
        workflow_text,
        "make test-profit-readiness",
    ), (
        "Expected .github/workflows/profit-readiness.yml to include a run step: "
        "'make test-profit-readiness'."
    )


def test_profit_readiness_workflow_verifies_clean_tree() -> None:
    workflow_text = _read_required_text(PROFIT_READINESS_WORKFLOW_PATH)

    assert _workflow_contains_run_command(
        workflow_text,
        "git diff --exit-code",
    ), (
        "Expected .github/workflows/profit-readiness.yml to include a clean-tree "
        "guard: 'git diff --exit-code'."
    )


def test_profit_readiness_workflow_trigger_paths_include_core_entries() -> None:
    workflow_text = _read_required_text(PROFIT_READINESS_WORKFLOW_PATH)
    push_paths = set(_extract_workflow_paths(workflow_text, "push"))
    pull_request_paths = set(_extract_workflow_paths(workflow_text, "pull_request"))

    for trigger_name, trigger_paths in (
        ("push", push_paths),
        ("pull_request", pull_request_paths),
    ):
        missing = sorted(PROFIT_READINESS_REQUIRED_TRIGGER_PATHS - trigger_paths)
        assert not missing, (
            "Missing required entries in on."
            f"{trigger_name}.paths for .github/workflows/profit-readiness.yml: {missing}."
        )


def test_profit_readiness_makefile_tests_are_covered_by_workflow_path_filters() -> None:
    makefile_text = _read_required_text(MAKEFILE_PATH)
    workflow_text = _read_required_text(PROFIT_READINESS_WORKFLOW_PATH)

    profit_readiness_tests = _parse_profit_readiness_tests_from_makefile(makefile_text)
    push_paths = set(_extract_workflow_paths(workflow_text, "push"))
    pull_request_paths = set(_extract_workflow_paths(workflow_text, "pull_request"))

    missing_in_push = [test_path for test_path in profit_readiness_tests if test_path not in push_paths]
    missing_in_pull_request = [
        test_path for test_path in profit_readiness_tests if test_path not in pull_request_paths
    ]

    assert not missing_in_push, (
        "These PROFIT_READINESS_TESTS entries are missing from on.push.paths in "
        f".github/workflows/profit-readiness.yml: {missing_in_push}. Sync workflow paths with Makefile."
    )
    assert not missing_in_pull_request, (
        "These PROFIT_READINESS_TESTS entries are missing from on.pull_request.paths in "
        f".github/workflows/profit-readiness.yml: {missing_in_pull_request}. Sync workflow paths with Makefile."
    )
