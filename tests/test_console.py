"""Console helpers that must behave without a live terminal."""

import builtins
import getpass
import io
import sys

from conversion.console import install_prompt_guard, spinner_paused


def test_prompt_guard_wraps_input_and_getpass_idempotently():
    install_prompt_guard()
    install_prompt_guard()  # must not double-wrap
    assert builtins.input.__name__ == "guarded_input"
    assert getpass.getpass.__name__ == "guarded_getpass"

    old_stdin = sys.stdin
    sys.stdin = io.StringIO("hello\n")
    try:
        assert builtins.input() == "hello"
    finally:
        sys.stdin = old_stdin


def test_spinner_paused_is_a_no_op_without_a_spinner():
    with spinner_paused():
        pass
