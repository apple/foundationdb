
docpreview:
	$(MAKE) -C sphinx html
	cp -r ../bindings/java/javadoc sphinx/.out/html
	cp -r ..//bindings/go/godoc sphinx/.out/html
	rm -f sphinx/.out/html/documentation
	ln -s . sphinx/.out/html/documentation
	@( cd sphinx/.out/html && ! grep FIXME * && python -m SimpleHTTPServer $$((0x$$(echo ${USER} | $(MD5SUM) | awk '{print $$1}' | cut -c1-8)%8000+8000)) )

docpreview_clean:
	$(MAKE) sphinx clean

docpackage:
	$(MAKE) -C sphinx html
	cp -r ../../foundationdb/bindings/java/javadoc sphinx/.out/html
	cp -r ../../foundationdb/bindings/go/godoc sphinx/.out/html
	rm -f sphinx/.out/html/documentation
	ln -s . sphinx/.out/html/documentation
	$(MAKE) -C sphinx package
