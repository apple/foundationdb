# Makefile for Sphinx documentation
#
# local vars:
PROJECT_NAME	:= foundationdb-docs

GIT_HEAD_REF	:= $(shell git rev-parse --short HEAD)
GIT_BRANCH	:= $(shell git symbolic-ref --short HEAD)
GIT_REPO_URL	:= $(shell git config --get remote.origin.url)

# You can set these variables from the command line.
SPHINXOPTS	:=  -c .
PAPER		=
ROOTDIR		:= $(CURDIR)
BUILDDIR	:= $(ROOTDIR)/.out
DISTDIR		:= $(ROOTDIR)/.dist
VENVDIR		:= $(ROOTDIR)/.out/venv
SPHINXBUILD	= $(VENVDIR)/bin/sphinx-build
SPHINXAUTOBUILD	= $(VENVDIR)/bin/sphinx-autobuild
TEMPLATEDIR	= $(ROOTDIR)/_templates

# virtualenv for sphinx-build
VENV_VERSION	?= virtualenv-13.0.1
VENV_URL_BASE	?= https://pypi.python.org
VENV_URL	?= $(VENV_URL_BASE)/packages/source/v/virtualenv/$(VENV_VERSION).tar.gz

# Internal variables.
PAPEROPT_a4	= -D latex_paper_size=a4
PAPEROPT_letter	= -D latex_paper_size=letter
ALLSPHINXOPTS	= -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) source
# the i18n builder cannot share the environment and doctrees with the others
I18NSPHINXOPTS	= $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) source

BUILDINFO = "<small><hr/><table><tr><th>Ref:</th><td><a href='$(GIT_REPO_URL)commit/%H'><em>%h</em></a></td></tr><tr><th>Updated:</th><td><em>%cd</em></td></tr><tr><th>Committer:</th><td><a href='mailto:%ce'>%cn</a></td></tr></table><br/><center><em><a href='$(GIT_REPO_URL)'>View on GitHub</a></em></center><hr/></small>"

.PHONY: default help clean html dirhtml singlehtml pickle json htmlhelp qthelp devhelp epub latex latexpdf text man changes linkcheck doctest gettext buildsphinx publish uptodate

default: html

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "	html		to make standalone HTML files"
	@echo "	livehtml	to launch a local webserver that auto-updates as changes are made"
	@echo "	publish		to build the html and push it to GitHub pages"
	@echo "	dirhtml		to make HTML files named index.html in directories"
	@echo "	singlehtml	to make a single large HTML file"
	@echo "	pickle		to make pickle files"
	@echo "	json		to make JSON files"
	@echo "	htmlhelp	to make HTML files and a HTML help project"
	@echo "	qthelp		to make HTML files and a qthelp project"
	@echo "	devhelp		to make HTML files and a Devhelp project"
	@echo "	epub		to make an epub"
	@echo "	latex		to make LaTeX files, you can set PAPER=a4 or PAPER=letter"
	@echo "	latexpdf	to make LaTeX files and run them through pdflatex"
	@echo "	latexpdfja	to make LaTeX files and run them through platex/dvipdfmx"
	@echo "	text		to make text files"
	@echo "	man		to make manual pages"
	@echo "	texinfo		to make Texinfo files"
	@echo "	info		to make Texinfo files and run them through makeinfo"
	@echo "	gettext		to make PO message catalogs"
	@echo "	changes		to make an overview of all changed/added/deprecated items"
	@echo "	xml		to make Docutils-native XML files"
	@echo "	pseudoxml	to make pseudoxml-XML files for display purposes"
	@echo "	linkcheck	to check all external links for integrity"
	@echo "	doctest		to run all doctests embedded in the documentation (if enabled)"
	@echo "	buildsphinx 	to install sphinx binary in virtualenv"

buildsphinx:
	if [ ! -e $(SPHINXBUILD) ]; then \
		mkdir $(BUILDDIR); \
		cd $(BUILDDIR); \
		curl -OL $(VENV_URL); \
		tar zxvf $(VENV_VERSION).tar.gz; \
		./$(VENV_VERSION)/virtualenv.py venv; \
	fi
	. $(VENVDIR)/bin/activate && \
	cp .pip.conf $(VENVDIR)/pip.conf && \
	pip install --upgrade pip && \
	pip install --upgrade -r $(ROOTDIR)/requirements.txt;

clean:
	rm -rf $(BUILDDIR)

cleanhtml:
	rm -rf $(BUILDDIR)/html

cleanvirtualenv:
	rm -rf $(VENVDIR)

html: buildsphinx cleanhtml
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

check: checkwarnings linkcheck

checkwarnings: buildsphinx
	$(SPHINXBUILD) -n -W -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo "Check finished."

livehtml: html
	$(SPHINXAUTOBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html

# removed html prerequisite because it is previously explictly invoked
package:
	mkdir -p $(DISTDIR)
	rm -f $(DISTDIR)/$(PROJECT_NAME)-$(VERSION).tar.gz
	cd $(BUILDDIR)/html && tar czf $(DISTDIR)/$(PROJECT_NAME)-$(VERSION).tar.gz .
