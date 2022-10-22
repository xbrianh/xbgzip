include common.mk
MODULES=bgzip tests
tests:=$(wildcard tests/test_*.py)

test: lint mypy bgzip_utils $(tests)

profile: bgzip_utils
	python dev_scripts/profile.py

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy $(MODULES)

$(tests): %.py:
	python $*.py

version: bgzip/version.py

bgzip/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	git clean -dfx

bgzip_utils.c: clean
	cython bgzip_utils/bgzip_utils.pyx

bgzip_utils: clean
	BUILD_WITH_CYTHON=1 python setup.py build_ext --inplace

build: clean version
	BUILD_WITH_CYTHON=1 python setup.py bdist_wheel

sdist: clean version bgzip_utils.c
	python setup.py sdist

install: build
	pip install --upgrade dist/*.whl

.PHONY: test profile lint mypy $(tests) clean build sdist install
