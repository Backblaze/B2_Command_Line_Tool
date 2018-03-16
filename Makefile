help: ## Show this help.
	@fgrep -h "##" Makefile | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'
	@echo ""
	@echo "To see doc options, cd to doc and type \"make\""

.PHONY: setup
setup: ## Set up (to run tests) using your current python environment
	python -m pip install -r requirements-test.txt

.PHONY: test
test:  ## Run unit tests
	./run-unit-tests.sh

.PHONY: clean
clean: ## Remove stuff you can regenerate
	rm -rf b2.egg-info build TAGS
	find . -name \*~ | xargs rm -f
	find . -name \*.pyc | xargs rm -f
