from setuptools import find_packages, setup
from setuptools.command.install import install

VERSION = "1.0.0"

pkgs = [
    "click",
    "openhunt",
    "streamlit",
    "pandas",
]

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="sysxtract",
    url="https://github.com/Ashton-Sidhu/sysmon-extract",
    packages=find_packages(),
    author="Ashton Sidhu",
    author_email="sidhuashton@gmail.com",
    install_requires=pkgs,
    version=VERSION,
    license="MIT",
    description="Extract logs based off events from sysmon. Comes as a package, cli and ui.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="datascience, security, infosec, analysis, pyspark, bigdata",
    python_requires=">= 3.6",
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    entry_points={"console_scripts": ["sysxtract=sysmon_extract.cli:cli"]},
)
