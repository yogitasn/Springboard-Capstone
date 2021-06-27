"""Setup file
Tutorial:
  http://the-hitchhikers-guide-to-packaging.readthedocs.io/en/latest/quickstart.html
rm -rf dist/
python setup.py sdist bdist_wheel
cd ..
pip install -I dist/Datasetprocessing-1.0-py3-none-any.whl  # Must be outside the project root
cd Datasetprocessing
"""
import setuptools  # this is for bdist wheel

from distutils.core import setup


setuptools.setup(
    name="Datasetprocessing",
    version=1.,
    author_email="yogitasn@yahoo.com",
    url="",
    packages=setuptools.find_packages(),
    package_dir={
        "dataset_processing": "dataset_processing",
        "dataset_processing.blockface_processing": "dataset_processing/blockface_processing",
        "dataset_processing.occupancy_processing": "dataset_processing/occupancy_processing",
        "dataset_processing.job_tracker": "dataset_processing/job_tracker",
        "dataset_processing.utilities": "dataset_processing/utilities",
        "tests": "tests"
        
    },
    python_requires=">=3.7",
    package_data={'occupancy': ['dataset_processing/data/occupancy.json'],
                  'blockface': ['dataset_processing/data/blockface.json'],
                  'setup': ['dataset_processing/setup.ini']},
    include_package_data=True,
    license=""
)
