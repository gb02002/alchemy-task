## This is a README

This repo contains a small project that solves some SQLAlcmehy tasks. You are free to copy it and do everything you want.

### Structure:
**Directoty 'code'** has 3 main '.py' files: **raw.py**, **task.py** and *others_and_setUps.py*.

**task.py** has best working solutions, and others_and_setUps.py has script for creating tables, populating them and other ways to solve problem from the task description. raw.py has the most close way to your expectation, so I would consider it as the main answer.

### How to set it up:

  1. Clone repo: *git clone https://github.com/gb02002/alchemy-task/*.

  2. Create venv: *'python -m venv /path/to/venv/'*.

  3. Acticate venv: *'Source venv/bin/activate'*

  4. Install dependencies: *'pip install -r requirements.txt'*.

Now it should work.

### How to run it.

  *Others_and_setup.py* file configures engine(line 19) and populates db with *create_and_populate_it()* func. If your database is ready, just fill your POSTGRES_values and run '*python task.py*', otherwise you run *others_and_setup.py*: *'python others_and_setup.py'*.

You can use your .env file and pass values.

## Overview:

*others_and_setup.py* has configuration scripts. 
*task.py* solves 2 questions and has it is runnable and convenient way, as I would store it in the project for instance. 
**raw.py** has everything in the **plain** way. Might be better in terms of examination.
