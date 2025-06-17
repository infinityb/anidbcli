{ python3Packages }:
python3Packages.buildPythonPackage {
  name = "anidbcli";
  src = ./anidbcli;
  format = "pyproject";
  propagatedBuildInputs = let py = python3Packages; in [
    py.click
    py.pycryptodome
    py.colorama
    py.pyperclip
    py.joblib
    py.sqlalchemy
    py.setuptools
  ];
}


