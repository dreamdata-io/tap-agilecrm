#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-agilecrm",
    version="0.0.1",
    description="Singer.io tap for extracting data AgileCMS",
    author="Dreamdata",
    url="https://dreamdata.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_mongodb"],
    install_requires=["singer-python==5.8.0", "requests==2.22.0"],
    entry_points="""
          [console_scripts]
          tap-agilecrm=agilecrm:main
      """,
    packages=["tap_agilecrm"],
)

