FDBLibTLS_CFLAGS := -fPIC -I/usr/local/include -I$(BOOSTDIR) -Ifdbrpc
FDBLibTLS_STATIC_LIBS := -ltls -lssl -lcrypto
FDBLibTLS_LDFLAGS := -L/usr/local/lib -static-libstdc++ -static-libgcc -lrt
FDBLibTLS_LDFLAGS += -Wl,-soname,FDBLibTLS.so -Wl,--version-script=FDBLibTLS/FDBLibTLS.map

# The plugin isn't a typical library, so it feels more sensible to have a copy
# of it in bin/.
bin/fdb-libressl-plugin.$(DLEXT): lib/libFDBLibTLS.$(DLEXT)
	@cp $< $@

TARGETS += bin/fdb-libressl-plugin.$(DLEXT)
