.PHONY: tests
tests:
	pytest -v -s tests/ --cov=tests/
