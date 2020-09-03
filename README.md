

## Prerequisites
1. Git
2. Python - Preferrably Python3.7
3. Virtualenv



## Getting started

This project uses Pybuilder for build automation

1. git clone 
2. cd 
3. Python3.7 -m venv env
4. source env/bin/activate
5. pip install pybuilder
6. pyb install_dependencies
7. Assuming that you are in project folder:
   python src/main/python/yde/dbinit.py
   (Do not skip this step as this downloads database to the project folder which is essential to run test cases)
8. pyb verify (for running test cases)
9. python src/main/python/yde/sqloperation.py
   (This runs all test cases as mentioned in the word document)





