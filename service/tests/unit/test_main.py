"""The CLI contract the grading harness relies on (on success, stdout carries exactly one line).
The run itself is replaced."""

from autoscaler import main as main_module
from autoscaler.application import RunSummary


def test_success_prints_only_the_completion_line_and_exits_0(monkeypatch, capsys):
    monkeypatch.setattr(
        main_module, "run", lambda settings: RunSummary(clusters=2, decisions=3, skipped_files=())
    )

    main_module.main()  # returning normally is exit code 0

    assert capsys.readouterr().out == "ALL CLUSTERS PROCESSED\n"
