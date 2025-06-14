#!/usr/bin/env python3

assertion_count = 0


def pytest_assertion_pass(item, lineno, orig, expl):
    global assertion_count
    assertion_count += 1


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    print(f'{assertion_count} assertions tested.')
