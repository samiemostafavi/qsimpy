import pathlib

from setuptools import find_packages, setup

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()
setup(
    name="qsimpy",
    version="0.0.1",
    description="Queueing theory simulation environment made with SimPy",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Seyed Samie Mostafavi",
    author_email="samiemostafavi@gmail.com",
    license="MIT",
    packages=find_packages(),
    zip_safe=False,
)
