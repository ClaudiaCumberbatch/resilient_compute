.PHONY: all
all: ;

.PHONY: clean
clean: 
	rm -rf runs cmd* temp* 

.PHONY: run
run:
	./run.sh 2>&1 | tee terminal.log

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make clean      - Remove all temporary files."
	@echo "  make run        - Run the main.py script."
