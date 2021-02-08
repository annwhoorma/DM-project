from setuptools import setup
from Cython.Build import cythonize
from shutil import copyfile
from os import remove, path

# files from ../src/
# setup
setup(
  name = 'myproject',
  ext_modules = cythonize(["../src/*.py"]),
)
