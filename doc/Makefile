# Minimal makefile for Sphinx documentation
#
MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= python3 -m sphinx
SOURCEDIR     = source
BUILDDIR      = build
WORKING_DIR 			:= $(dir $(MKFILE_PATH))

# Define DOXYGEN command, allowing override
DOXYGEN       ?= doxygen

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile dependencies doxygen python-docs cpp-docs api-docs html-only

dependencies:
	python3 -m pip  install -r docs-requirements.txt

# Generate Doxygen XML (for C++ API)
doxygen:
	@echo "Generating Doxygen XML for C++ API..."
	@mkdir -p $(BUILDDIR)/html/reference
	@command -v $(DOXYGEN) >/dev/null 2>&1 || \
		{ echo >&2 "Error: \"$(DOXYGEN)\" not found. Please install Doxygen or ensure it's in your PATH."; exit 1; }
	@$(DOXYGEN) $(SOURCEDIR)/Doxyfile
	@echo "✅ Doxygen XML generation completed!"

# Generate Python API documentation only
python-docs:
	@echo "Generating Python API docs with pydoc-markdown..."
	@cd $(WORKING_DIR) && python3 $(SOURCEDIR)/_scripts/generate_python_docs.py
	@echo "✅ Python API documentation generated!"

# Generate C++ API documentation only
cpp-docs: doxygen
	@echo "Generating C++ API docs..."
	@cd $(WORKING_DIR) && python3 $(SOURCEDIR)/_scripts/generate_cpp_docs.py
	@echo "✅ C++ API documentation generated!"

# Generate all API documentation (Python + C++)
api-docs: python-docs cpp-docs
	@echo "✅ All API documentation generated!"

# Build only Sphinx documentation (without regenerating Doxygen)
html-only:
	@BUILD_DOC=ON $(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# Build complete documentation (Doxygen + API docs + Sphinx)
html: api-docs html-only

# Catch-all target: route other targets to Sphinx only
%: Makefile
	@BUILD_DOC=ON $(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
