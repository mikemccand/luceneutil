# basic make safety: if error happens, delete output
.DELETE_ON_ERROR:

# explicitly declare shell used
SHELL := /bin/bash

# enforce that shell is picky
.SHELLFLAGS := -norc -euo pipefail -c

# don't litter up repo with bytecode
export PYTHONDONTWRITEBYTECODE=true

# don't self-check, don't ask questions
PIP_INSTALL_ARGS=--disable-pip-version-check --no-input --upgrade

# venv with dependencies in the standard location
VENV=${PWD}/.venv

# don't behave strangely if these files exist
.PHONY: lint format reformat tidy autofix ruff ruff-fix pyright test env clean native native-run native-clean

# native vectorized dot product example, see native/README.md
CC ?= gcc
UNAME_S := $(shell uname -s)
ifeq ($(findstring MINGW,$(UNAME_S))$(findstring MSYS,$(UNAME_S)),)
  ifeq ($(UNAME_S),Darwin)
    NATIVE_LIB_EXT=dylib
  else
    NATIVE_LIB_EXT=so
  endif
else
  NATIVE_LIB_EXT=dll
endif
NATIVE_LIB=build/native/libdot_product.$(NATIVE_LIB_EXT)
NATIVE_CFLAGS=-O3 -march=native -ffast-math -fPIC -shared

# builds the native dot product library, tuned for the cpu running make (no cross compilation)
native: $(NATIVE_LIB)
$(NATIVE_LIB): native/dot_product.c
	mkdir -p build/native
	$(CC) $(NATIVE_CFLAGS) -o $(NATIVE_LIB) native/dot_product.c
	@echo "built $(NATIVE_LIB) with $(CC) $(NATIVE_CFLAGS)"

# builds then calls the native library from java (needs java 22+); look for the "RUNNING" message
native-run: native
	java --enable-native-access=ALL-UNNAMED native/NativeDotProduct.java $(NATIVE_LIB)

native-clean:
	rm -rf build/native

# list of directories we check
SOURCES=src/python

# check formatting, linting, and types
lint: format ruff pyright

# check all formatting
format: env
	# validate imports: if this fails, please run "make reformat" and commit changes.
	$(VENV)/bin/ruff check --select I $(SOURCES)
	# validate formatting: if this fails, please run "make reformat" and commit changes.
	$(VENV)/bin/ruff format --diff $(SOURCES)

# alias for lucene gradle users!
tidy: reformat

# reformat code to conventions
reformat: env
	# organize imports
	$(VENV)/bin/ruff check --select I --fix $(SOURCES)
	# reformat sources
	$(VENV)/bin/ruff format $(SOURCES)

# applies all safe fixes, if successful reformats too
autofix: ruff-fix reformat

# applies safe autofixes
ruff-fix: env
	# fix sources with ruff
	$(VENV)/bin/ruff check --preview --fix $(SOURCES)

# lints sources
ruff: env
	# validate sources with ruff linter
	$(VENV)/bin/ruff check --preview $(SOURCES)

# checks types
pyright: env
	# type-check sources with basedpyright
	$(VENV)/bin/basedpyright $(SOURCES)

# runs unit tests
test: env
	# run tests with the venv python
	$(VENV)/bin/python -m unittest discover -s tests -q

# rebuild venv if dependencies change
env: $(VENV)/bin/activate
$(VENV)/bin/activate: requirements.txt
	# remove any existing venv
	rm -rf $(VENV)
	# create new venv
	python3 -m venv $(VENV)
	# install dependencies into venv
	$(VENV)/bin/pip install $(PIP_INSTALL_ARGS) -r requirements.txt
	# adjust timestamp for safety
	touch $(VENV)/bin/activate

# nuke venv
clean:
	rm -rf $(VENV)
