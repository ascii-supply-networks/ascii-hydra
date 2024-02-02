from setuptools import find_packages, setup

dagster_version = ">=1.6.0,<1.7"


setup(
    name="ascii_library",
    packages=find_packages(),
    install_requires=[
        f"dagster {dagster_version}",
        f"dagster-pipes {dagster_version}",
        "boto3",
    ],
)
