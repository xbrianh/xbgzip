include common.mk

MODULES=xbgzip tests

test: lint mypy xbgzip_utils tests

profile: xbgzip_utils
	python dev_scripts/profile.py

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=xbgzip \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

version: xbgzip/version.py

xbgzip/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	git clean -dfx

xbgzip_utils.c: clean
	cython xbgzip_utils/xbgzip_utils.pyx

xbgzip_utils: clean
	BUILD_WITH_CYTHON=1 python setup.py build_ext --inplace

build: clean version
	BUILD_WITH_CYTHON=1 python setup.py bdist_wheel

sdist: clean version xbgzip_utils.c
	python setup.py sdist

install: build
	pip install --upgrade dist/*.whl

.PHONY: test profile lint mypy tests clean build sdist install
