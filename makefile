.PHONY: tests
tests:
	pytest -vv -s tests/ --cov=tests/
