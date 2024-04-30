.PHONY: all
all: ;

.PHONY: clean
clean: 
	rm -rf runinfo cmd* temp*

.PHONY: run
run:
	python main.py > terminal.log 2>&1

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make clean      - Remove all temporary files."
	@echo "  make run        - Run the main.py script."