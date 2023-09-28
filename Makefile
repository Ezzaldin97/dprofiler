pylint:
	bash linting.sh

test:
	bash testing.sh

docstrings:
	interrogate -v ./qprofiler