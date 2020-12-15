lint:
	@flake8 --exclude="./build/" --ignore="E501,F401,F403,F405,E402"

publish:
	python3.8 setup.py sdist upload -r pypi
