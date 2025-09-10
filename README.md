# Introduction
This repo is an effort/challenge/exercise to replicate Django's ORM. I have used this in production some times. This is not a full fledge ORM as I wrote this for Sqlite only.

The package include different parts:
- `models.py`, at the core of which is `BasicModel` class, an endpoint to work with a data model supporting different methods: create object, validate data entry, create table, query object, etc;
- `datatypes.py` includes different data type classes which could be use with `BasicModel`. Every classes have similar methods such as for updating value, validating value, checking for foreign key, etc.
- `sqlite_driver.py`: a simple wrapper for `sqlite3`