
ifeq ($(PLATFORM),freebsd)
FDBLibTLS_CFLAGS := -fPIC -I/usr/local/include -I$(BOOSTDIR)/include
FDBLibTLS_LIBS := -lssl -lcrypto
FDBLibTLS_LDFLAGS := -lc++
else
FDBLibTLS_CFLAGS := -fPIC -I/usr/local/include -I$(BOOSTDIR)
FDBLibTLS_STATIC_LIBS := -ltls -lssl -lcrypto
FDBLibTLS_LDFLAGS := -L/usr/local/lib -static-libstdc++ -static-libgcc -lrt
endif

FDBLibTLS_LDFLAGS += -Wl,-soname,FDBLibTLS.so -Wl,--version-script=FDBLibTLS/FDBLibTLS.map

# The plugin isn't a typical library, so it feels more sensible to have a copy
# of it in bin/.
bin/FDBLibTLS.$(DLEXT): lib/libFDBLibTLS.$(DLEXT)
	@cp $< $@

TARGETS += bin/FDBLibTLS.$(DLEXT)
