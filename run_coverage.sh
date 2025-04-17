#!/bin/bash

coverage run --source=src -m pytest && coverage report -m && coverage html && open htmlcov/index.html