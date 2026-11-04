PYTHON ?= python

.PHONY: install lint test check

install:
	$(PYTHON) -m pip install -e '.[dev]'

lint:
	$(PYTHON) -m ruff check .

test:
	$(PYTHON) -m pytest -q

check: lint test
