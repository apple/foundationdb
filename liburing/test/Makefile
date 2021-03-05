prefix ?= /usr
datadir ?= $(prefix)/share

INSTALL=install

CPPFLAGS ?=
override CPPFLAGS += -D_GNU_SOURCE -D__SANE_USERSPACE_TYPES__ \
	-I../src/include/ -include ../config-host.h
CFLAGS ?= -g -O2
XCFLAGS =
override CFLAGS += -Wall -Wextra -Wno-unused-parameter -Wno-sign-compare \
	-L../src/
CXXFLAGS ?=
override CXXFLAGS += $(CFLAGS) -std=c++11

test_targets += \
	232c93d07b74-test \
	35fa71a030ca-test \
	500f9fbadef8-test \
	7ad0e4b2f83c-test \
	8a9973408177-test \
	917257daa0fe-test \
	a0908ae19763-test \
	a4c0b3decb33-test \
	accept \
	accept-link \
	accept-reuse \
	accept-test \
	across-fork splice \
	b19062a56726-test \
	b5837bd5311d-test \
	ce593a6c480a-test \
	close-opath \
	connect \
	cq-full \
	cq-overflow \
	cq-overflow-peek \
	cq-peek-batch \
	cq-ready \
	cq-size \
	d4ae271dfaae-test \
	d77a67ed5f27-test \
	defer \
	double-poll-crash \
	eeed8b54e0df-test \
	eventfd \
	eventfd-disable \
	eventfd-ring \
	fadvise \
	fallocate \
	fc2a85cb02ef-test \
	file-register \
	file-update \
	files-exit-hang-poll \
	files-exit-hang-timeout \
	fixed-link \
	fsync \
	io-cancel \
	io_uring_enter \
	io_uring_register \
	io_uring_setup \
	iopoll \
	lfs-openat \
	lfs-openat-write \
	link \
	link-timeout \
	link_drain \
	madvise \
	nop \
	nop-all-sizes \
	open-close \
	openat2 \
	personality \
	pipe-eof \
	pipe-reuse \
	poll \
	poll-cancel \
	poll-cancel-ton \
	poll-link \
	poll-many \
	poll-v-poll \
	probe \
	read-write \
	register-restrictions \
	ring-leak \
	send_recv \
	send_recvmsg \
	shared-wq \
	short-read \
	shutdown \
	sigfd-deadlock \
	socket-rw \
	socket-rw-eagain \
	sq-full \
	sq-poll-dup \
	sq-poll-kthread \
	sq-poll-share \
	sq-space_left \
	stdout \
	submit-reuse \
	teardowns \
	timeout \
	timeout-overflow \
	wakeup-hang \
	# EOL

all_targets += $(test_targets)

include ../Makefile.quiet

ifneq ($(MAKECMDGOALS),clean)
include ../config-host.mak
endif

ifdef CONFIG_HAVE_STATX
test_targets += statx
endif
all_targets += statx

ifdef CONFIG_HAVE_CXX
test_targets += sq-full-cpp
endif
all_targets += sq-full-cpp

all: $(test_targets)

%: %.c
	$(QUIET_CC)$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $< -luring $(XCFLAGS)

%: %.cc
	$(QUIET_CXX)$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< -luring $(XCFLAGS)

test_srcs := \
	232c93d07b74-test.c \
	35fa71a030ca-test.c \
	500f9fbadef8-test.c \
	7ad0e4b2f83c-test.c \
	8a9973408177-test.c \
	917257daa0fe-test.c \
	a0908ae19763-test.c \
	a4c0b3decb33-test.c \
	accept-link.c \
	accept-reuse.c \
	accept-test.c \
	accept.c \
	across-fork.c \
	b19062a56726-test.c \
	b5837bd5311d-test.c \
	ce593a6c480a-test.c \
	close-opath.c \
	connect.c \
	cq-full.c \
	cq-overflow-peek.c \
	cq-overflow.c \
	cq-peek-batch.c \
	cq-ready.c\
	cq-size.c \
	d4ae271dfaae-test.c \
	d77a67ed5f27-test.c \
	defer.c \
	double-poll-crash.c \
	eeed8b54e0df-test.c \
	eventfd-disable.c \
	eventfd-ring.c \
	eventfd.c \
	fadvise.c \
	fallocate.c \
	fc2a85cb02ef-test.c \
	file-register.c \
	file-update.c \
	files-exit-hang-poll.c \
	files-exit-hang-timeout.c \
	fixed-link.c \
	fsync.c \
	io-cancel.c \
	io_uring_enter.c \
	io_uring_register.c \
	io_uring_setup.c \
	iopoll.c \
	lfs-openat-write.c \
	lfs-openat.c \
	link-timeout.c \
	link.c \
	link_drain.c \
	madvise.c \
	nop-all-sizes.c \
	nop.c \
	open-close.c \
	openat2.c \
	personality.c \
	pipe-eof.c \
	pipe-reuse.c \
	poll-cancel-ton.c \
	poll-cancel.c \
	poll-link.c \
	poll-many.c \
	poll-v-poll.c \
	poll.c \
	probe.c \
	read-write.c \
	register-restrictions.c \
	ring-leak.c \
	send_recvmsg.c \
	shared-wq.c \
	short-read.c \
	shutdown.c \
	sigfd-deadlock.c \
	socket-rw.c \
	socket-rw-eagain.c \
	splice.c \
	sq-full-cpp.cc \
	sq-full.c \
	sq-poll-dup.c \
	sq-poll-kthread.c \
	sq-poll-share.c \
	sq-space_left.c \
	statx.c \
	stdout.c \
	submit-reuse.c \
	teardowns.c \
	timeout-overflow.c \
	timeout.c \
	wakeup-hang.c \
	# EOL

test_objs := $(patsubst %.c,%.ol,$(patsubst %.cc,%.ol,$(test_srcs)))

35fa71a030ca-test: XCFLAGS = -lpthread
232c93d07b74-test: XCFLAGS = -lpthread
send_recv: XCFLAGS = -lpthread
send_recvmsg: XCFLAGS = -lpthread
poll-link: XCFLAGS = -lpthread
accept-link: XCFLAGS = -lpthread
submit-reuse: XCFLAGS = -lpthread
poll-v-poll: XCFLAGS = -lpthread
across-fork: XCFLAGS = -lpthread
ce593a6c480a-test: XCFLAGS = -lpthread
wakeup-hang: XCFLAGS = -lpthread
pipe-eof: XCFLAGS = -lpthread

install: $(test_targets) runtests.sh runtests-loop.sh
	$(INSTALL) -D -d -m 755 $(datadir)/liburing-test/
	$(INSTALL) -D -m 755 $(test_targets) $(datadir)/liburing-test/
	$(INSTALL) -D -m 755 runtests.sh  $(datadir)/liburing-test/
	$(INSTALL) -D -m 755 runtests-loop.sh  $(datadir)/liburing-test/
clean:
	@rm -f $(all_targets) $(test_objs)

runtests: all
	@./runtests.sh $(test_targets)
runtests-loop: all
	@./runtests-loop.sh $(test_targets)
