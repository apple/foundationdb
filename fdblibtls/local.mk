fdblibtls_CFLAGS := -fPIC -I/usr/local/include -I$(BOOSTDIR)
fdblibtls_STATIC_LIBS := -ltls -lssl -lcrypto
fdblibtls_LDFLAGS := -L/usr/local/lib -static-libstdc++ -static-libgcc -lrt
fdblibtls_LDFLAGS += -Wl,-soname,fdblibtls.so -Wl,--version-script=fdblibtls/fdblibtls.map

# The plugin isn't a typical library, so it feels more sensible to have a copy
# of it in bin/.
bin/fdblibtls.$(DLEXT): lib/libfdblibtls.$(DLEXT)
	@cp $< $@

TARGETS += bin/fdblibtls.$(DLEXT)
