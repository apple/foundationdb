default_target: mako

#FDB_API_VERSION=307
#FDBDIR=/home/kmakino/scratch/rpms/3.2.2
FDB_API_VERSION=610
FDBDIR=/home/kmakino/scratch/rpms/6.1.0

INCS = -I. -I$(FDBDIR)/usr/include

CFLAGS = -g -fPIC -O2 $(INCS) -DFDB_API_VERSION=$(FDB_API_VERSION)
LDFLAGS = -L$(FDBDIR)/usr/lib64 -lm -lpthread -lrt -lfdb_c

TARGET = mako
SRCS = mako.c utils.c zipf.c
OBJS := $(SRCS:.c=.o)

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

clean:
	$(RM) $(TARGET) $(OBJS)
