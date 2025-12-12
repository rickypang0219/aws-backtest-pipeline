


# Environemnt Variables (Homebrew Install)
- export JAVA_HOME=$(which java)
- export SPARK_HOME=$(which spark)
- export PYSPARK_PYTHON=$(which python)

Command before running PySpark: `export JAVA_HOME=$(which java) && export SPARK_HOME=$(which spark) && export PYSPARK_PYTHON=$(which python)`


# Nix OS Setting (zsh.nix)
- export JAVA_HOME={pkgs.jdk8}
- export SPARK_HOME={pkgs.spark}
- export PYSPARK_PYTHON=$(which python) # python venv

The `PySpark` lives in `.venv` should have the same version as `Spark` installed via Nix Flakes or Homebrew.


# References
- https://medium.com/@jpurrutia95/install-and-set-up-pyspark-in-5-minutes-m1-mac-eb415fe623f3
