from setuptools import setup

setup(
    name = "celery-mongodb-nobinary-backend",
    description = "Celery backend for MongoDB that does not convert task results into a binary format before storing them in Mongo database.",
    version = "0.0.15",
    license = "Apache License, Version 2.0",
    author = "Zakir Durumeric",
    author_email = "zakird@gmail.com",
    maintainer = "Zakir Durumeric",
    maintainer_email = "zakird@gmail.com",

    keywords = "python celery mongodb backend",

    packages = [
        "zcelerybackend"
    ],

    install_requires=[
        'setuptools',
        'celery'
    ]

)
