#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-agilecrm",
    version="0.0.1",
    description="Singer.io tap for extracting data AgileCMS",
    author="Dreamdata",
    url="https://dreamdata.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=["singer-python==5.8.0", "requests"],
    entry_points="""
        [console_scripts]
        tap-agilecrm=tap_agilecrm:main
    """,
    include_package_data=True,
    package_data={
        "./tap_agilecrm/schemas": [
            "company_schema_infer.json",
            "contact_schema_infer.json",
            "deal_schema_infer.json",
        ]
    },
    packages=["tap_agilecrm"],
    setup_requires=["pytest-runner"],
    extras_require={
        "test": [
            ["pytest", "google.cloud.bigquery"]
        ]
    },
)

