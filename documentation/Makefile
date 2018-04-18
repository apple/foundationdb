include ../build/scver.mk

docprereqs:
	$(MAKE) -C sphinx html
	cp -r ../bindings/java/javadoc sphinx/.out/html
	rm -f sphinx/.out/html/documentation
	ln -s . sphinx/.out/html/documentation

docpreview: docprereqs
	@( cd sphinx/.out/html && ! grep FIXME * && python -m SimpleHTTPServer $$((0x$$(echo ${USER} | $(MD5SUM) | awk '{print $$1}' | cut -c1-8)%8000+8000)) )

docpreview_clean:
	$(MAKE) -C sphinx clean

docpackage: docprereqs
	$(MAKE) -C sphinx package
