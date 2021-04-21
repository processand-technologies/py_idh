#!/usr/bin/python3

import os
from setuptools import setup, find_packages, Extension

with open("requirements.txt") as fp:
    install_requires = fp.read().strip().split("\n")
print(find_packages(exclude=['tests*']))
setup(
    ext_modules=[],
    install_requires=install_requires,
    packages=find_packages(exclude=['tests*']),
    include_package_data=True
)
