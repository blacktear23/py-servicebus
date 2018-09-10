lint:
	@flake8 --exclude="./build/" --ignore="E501,F401,F403,F405,E402"

publish:
	python setup.py sdist upload -r pypi
