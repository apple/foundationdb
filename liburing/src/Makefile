prefix ?= /usr
includedir ?= $(prefix)/include
libdir ?= $(prefix)/lib
libdevdir ?= $(prefix)/lib

CPPFLAGS ?=
override CPPFLAGS += -Iinclude/ -include ../config-host.h
CFLAGS ?= -g -fomit-frame-pointer -O2
override CFLAGS += -Wall -Wextra -Wno-unused-parameter -Wno-sign-compare
SO_CFLAGS=-fPIC $(CFLAGS)
L_CFLAGS=$(CFLAGS)
LINK_FLAGS=
LINK_FLAGS+=$(LDFLAGS)
ENABLE_SHARED ?= 1

soname=liburing.so.2
minor=0
micro=0
libname=$(soname).$(minor).$(micro)
all_targets += liburing.a

ifeq ($(ENABLE_SHARED),1)
all_targets += $(libname)
endif

include ../Makefile.quiet

ifneq ($(MAKECMDGOALS),clean)
include ../config-host.mak
endif

all: $(all_targets)

liburing_srcs := setup.c queue.c syscall.c register.c

liburing_objs := $(patsubst %.c,%.ol,$(liburing_srcs))
liburing_sobjs := $(patsubst %.c,%.os,$(liburing_srcs))

$(liburing_objs) $(liburing_sobjs): include/liburing/io_uring.h

%.os: %.c
	$(QUIET_CC)$(CC) $(CPPFLAGS) $(SO_CFLAGS) -c -o $@ $<

%.ol: %.c
	$(QUIET_CC)$(CC) $(CPPFLAGS) $(L_CFLAGS) -c -o $@ $<

AR ?= ar
RANLIB ?= ranlib
liburing.a: $(liburing_objs)
	@rm -f liburing.a
	$(QUIET_AR)$(AR) r liburing.a $^
	$(QUIET_RANLIB)$(RANLIB) liburing.a

$(libname): $(liburing_sobjs) liburing.map
	$(QUIET_CC)$(CC) $(SO_CFLAGS) -shared -Wl,--version-script=liburing.map -Wl,-soname=$(soname) -o $@ $(liburing_sobjs) $(LINK_FLAGS)

install: $(all_targets)
	install -D -m 644 include/liburing/io_uring.h $(includedir)/liburing/io_uring.h
	install -D -m 644 include/liburing.h $(includedir)/liburing.h
	install -D -m 644 include/liburing/compat.h $(includedir)/liburing/compat.h
	install -D -m 644 include/liburing/barrier.h $(includedir)/liburing/barrier.h
	install -D -m 644 liburing.a $(libdevdir)/liburing.a
ifeq ($(ENABLE_SHARED),1)
	install -D -m 755 $(libname) $(libdir)/$(libname)
	ln -sf $(libname) $(libdir)/$(soname)
	ln -sf $(relativelibdir)$(libname) $(libdevdir)/liburing.so
endif

$(liburing_objs): include/liburing.h

clean:
	@rm -f $(all_targets) $(liburing_objs) $(liburing_sobjs) $(soname).new
	@rm -f *.so* *.a *.o
	@rm -f include/liburing/compat.h
