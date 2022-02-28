.PHONY: all

all: main.pdf

main.pdf: main.tex fdb.png requirement_i.png requirement_ii.png
	pdflatex main.tex
	pdflatex main.tex

fdb.png: fdb.dot
	dot fdb.dot -T png -Gsize=3.94\! -Gdpi=800 -ofdb.png

requirement_i.png: loop.dot
	dot loop.dot -T png -Gsize=1,3\! -Gdpi=800 -orequirement_i.png

requirement_ii.png: loop_multi_storage_team.dot
	dot loop_multi_storage_team.dot -T png -Gsize=1,3\! -Gdpi=800 -orequirement_ii.png

