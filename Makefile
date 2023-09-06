lint:
	black --check --diff . && ruff check --show-source .
	
test:
	pytest -v