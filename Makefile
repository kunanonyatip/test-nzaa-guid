# Detect Python command
PYTHON := $(shell command -v python3 || command -v python)
PIP := $(shell command -v pip3 || command -v pip)

.PHONY: test test-unit test-bigquery test-terraform deploy setup-test check-python install-deps

# Check if Python is available
check-python:
	@if [ -z "$(PYTHON)" ]; then \
		echo "Error: Python is not installed or not in PATH"; \
		echo "Please install Python 3 or set up your environment"; \
		exit 1; \
	fi
	@echo "Using Python: $(PYTHON)"

# Install test dependencies
install-deps: check-python
	@echo "Installing test dependencies..."
	$(PIP) install google-cloud-bigquery pandas pyarrow db-dtypes python-dotenv pytest google-cloud-bigquery-storage

# Setup test environment
setup-test: check-python install-deps
	@echo "Setting up test environment..."
	@test -f .env || cp .env.example .env
	@echo "Please update .env with your project details"

# Run all tests
test: check-python test-unit test-bigquery test-terraform

# Run unit tests
test-unit: check-python
	@if find cloud_functions -name "test_*.py" 2>/dev/null | grep -q .; then \
		echo "Running cloud function tests..."; \
		$(PYTHON) -m pytest cloud_functions/*/test_*.py -v; \
	else \
		echo "No cloud function tests found (skipping)"; \
	fi

# Run BigQuery procedure tests
test-bigquery: check-python
	$(PYTHON) -m unittest bigquery.procedures.test_update_identity_match.TestUpdateIdentityMatch -v

# Run specific test method
test-identity-insert: check-python
	$(PYTHON) -m unittest bigquery.procedures.test_update_identity_match.TestUpdateIdentityMatch.test_new_identity_insertion -v

# Run specific test method
test-identity-update: check-python
	$(PYTHON) -m unittest bigquery.procedures.test_update_identity_match.TestUpdateIdentityMatch.test_identity_update -v

# Run with pytest for better output
test-pytest: check-python
	$(PYTHON) -m pytest bigquery/procedures/test_update_identity_match.py -v -s

# Validate terraform
test-terraform:
	cd terraform && terraform init -backend=false && terraform validate

# Deploy with tests
deploy: test
	cd terraform && terraform apply -var-file=environments/$(ENV).tfvars

# Clean up test artifacts
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + || true
	find . -type f -name "*.pyc" -delete || true

# Show Python version and installed packages
python-info: check-python
	@$(PYTHON) --version
	@echo "\nInstalled packages:"
	@$(PIP) list | grep -E "(google-cloud-bigquery|pandas|db-dtypes|pyarrow)"