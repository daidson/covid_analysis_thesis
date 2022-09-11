# COVID Thesis

Hello, welcome to my COVID thesis repository.

## Description

Add a description here

## How to use

Add a how to use

## Dependencies

### Poetry

* Poetry works as a virtual environment on Python, in which you can use Python libraries without having to install them.
* To start using poetry on your local repository, you should start with: (ONLY USE THIS IF YOU STILL DON'T HAVE `pyproject.toml` locally)

```
$ poetry init
```

* In order to run poetry's virtual environment, the command needed is:

```
$ poetry shell
```

* Top stop your virtual environment's execution, run at anytime:

```
$ exit
```

* To add a new dependency on poetry, use:

```
$ poetry add [dependency name]
```

* To install the dependencies, use:

```
$ poetry install
```

* To remove a desired dependency, use:

```
$ poetry remove [dependency name]
```

* For more on the documentation, refer to: https://python-poetry.org/docs/cli/

### PyTest

* To run the tests and see coverage, use:

```
$ make tests
```
