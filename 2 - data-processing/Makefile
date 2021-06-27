install:
	pip install -r requirements.txt -I

lint:
	black dataset_processing tests -l 120 --target-version=py37

test:
	black dataset_processing dataset_processing -l 120 --target-version=py37 --check
	pytest -s -v --cov=dataset_processing tests --cov-fail-under=40 --disable-pytest-warnings