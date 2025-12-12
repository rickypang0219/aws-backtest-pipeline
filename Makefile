.PHONY: check fix freeze

check:
	@echo "Running check..."
	npm run format:check
	npm run lint

fix:
	@echo "Running fix..."
	npm run format
	npm run lint:fix

clean:
	@echo "Cleaning cdk.out"
	rm -rf cdk.out

# Default target
.DEFAULT_GOAL := check
