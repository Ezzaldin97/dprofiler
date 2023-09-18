pylint:
	bash linting.sh

test:
	pytest -v

docstrings:
	interrogate -v ./qprofiler