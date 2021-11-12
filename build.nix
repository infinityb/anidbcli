{ pkgs, python37, zip }:
{
	anidbcliZip = let
		python = python37.withPackages (python-packages: with python-packages; [
			click
			pycryptodome
			colorama
			pyperclip
			joblib
		]);
		main = ./main.py;
		src = ./anidbcli;
	in pkgs.runCommandNoCC "anidbcli" {} ''
		set -e
		cp -ar ${main} ./__main__.py
		cp -ar ${src} ./anidbcli
		${zip}/bin/zip -0 "tmp.zip" -r ./__main__.py ./anidbcli

		(
			echo "#!${python}/bin/python" &&
			cat "tmp.zip"
		) > $out
		chmod +x $out
	'';
}
