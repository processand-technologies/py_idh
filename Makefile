.PHONY: build dist redist install install-from-source clean uninstall

build:
	./setup.py build

dist:
	./setup.py sdist bdist_wheel

redist: clean dist

install:
	pip install .

install-from-source: dist
	pip install dist/py_idh-1.0.1.tar.gz

clean:
	$(RM) -r build dist py_idh/*.egg-info
	$(RM) -r .pytest_cache
	find . -name __pycache__ -exec rm -r {} +
	#git clean -fdX

uninstall:
	pip uninstall py_idh
