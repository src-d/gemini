# Package configuration
PROJECT = gemini
SBT = ./sbt

# Including ci Makefile
MAKEFILE = Makefile.main
CI_REPOSITORY = https://github.com/src-d/ci.git
CI_FOLDER = .ci

# Python configuration
YAPF = yapf
PYTHON_LINT_DIRS = src/main/python src/test/resources/weighted-minhash
YAPF_CMD = $(YAPF) --recursive $(PYTHON_LINT_DIRS) --parallel --exclude '*/pb/*'

$(MAKEFILE):
	@git clone --quiet $(CI_REPOSITORY) $(CI_FOLDER); \
	cp $(CI_FOLDER)/$(MAKEFILE) .;

-include $(MAKEFILE)

build:
	$(SBT) assembly
	$(SBT) package

format-python:
	$(YAPF_CMD) --in-place

lint-python:
	$(YAPF_CMD) --diff
