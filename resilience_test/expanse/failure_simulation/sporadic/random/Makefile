.PHONY: all
all: ;

.PHONY: clean
clean: 
	rm -rf runinfo cmd* temp*

.PHONY: run
run:
	python main.py 2>&1 | tee terminal.log

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make clean      - Remove all temporary files."
	@echo "  make run        - Run the main.py script."