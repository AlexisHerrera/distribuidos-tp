#!/bin/bash

# Run the tests; create the report and open coverage on browser
coverage run --source=src -m pytest && coverage report -m && coverage html && open htmlcov/index.html