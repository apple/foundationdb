PROJECTPATH = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
PLUGINPATH = $(PROJECTPATH)/$(PLUGIN)

CFLAGS ?= -O2 -g

CXXFLAGS ?= -std=c++0x

CFLAGS += -I/usr/local/include
LDFLAGS += -L/usr/local/lib

LIBS += -ltls -lssl -lcrypto

PLATFORM := $(shell uname)
ifneq ($(PLATFORM),Darwin)
	PLATFORM := $(shell uname -o)
endif

ifeq ($(PLATFORM),Cygwin)
	HOST := x86_64-w64-mingw32
	CC := $(HOST)-gcc
	CXX := $(HOST)-g++
	STRIP := $(HOST)-strip --strip-all

	DYEXT = dll
	PLUGINPATH = $(PLUGIN)

	LIBS += -static-libstdc++ -static-libgcc
	LIBS += -lws2_32

	LINK_LDFLAGS = -shared
	LINK_LDFLAGS += -Wl,-soname,$(PLUGIN)
	LINK_LDFLAGS += -Wl,--version-script=FDBLibTLS.map
	LINK_LDFLAGS += -Wl,-Bstatic $(LIBS) -Wl,-Bdynamic

else ifeq ($(PLATFORM),Darwin)
	CC := clang
	CXX := clang++
	STRIP := strip -S -x

	CFLAGS += -fPIC

	DYEXT = dylib

	vpath %.a /usr/local/lib
	.LIBPATTERNS = lib%.a lib%.dylib lib%.so

	LINK_LDFLAGS = -shared
	LINK_LDFLAGS += -Wl,-exported_symbols_list,FDBLibTLS.symbols
	LINK_LDFLAGS += -Wl,-dylib_install_name,$(PLUGIN)
	LINK_LDFLAGS += $(LIBS)

else ifeq ($(PLATFORM),GNU/Linux)
	CC := clang
	CXX := clang++
	STRIP := strip --strip-all

	CFLAGS += -fPIC
	DYEXT = so

	LIBS += -static-libstdc++ -static-libgcc -lrt

	LINK_LDFLAGS = -shared
	LINK_LDFLAGS += -Wl,-soname,$(PLUGIN)
	LINK_LDFLAGS += -Wl,--version-script=FDBLibTLS.map
	LINK_LDFLAGS += -Wl,-Bstatic $(LIBS) -Wl,-Bdynamic

else
$(error Unknown platform $(PLATFORM))
endif

PLUGIN := FDBLibTLS.$(DYEXT)
OBJECTS := FDBLibTLSPlugin.o FDBLibTLSPolicy.o FDBLibTLSSession.o FDBLibTLSVerify.o
LINKLINE := $(CXXFLAGS) $(CFLAGS) $(LDFLAGS) $(OBJECTS) $(LINK_LDFLAGS) -o $(PLUGIN)

all: $(PLUGIN)

build-depends-linux:
	apt install clang make libboost-dev

clean:
	@rm -f *.o *.d $(PLUGIN) plugin-test verify-test
	@rm -rf *.dSYM

DEPS := $(patsubst %.o,%.d,$(OBJECTS))
-include $(DEPS)

$(OBJECTS): %.o: %.cpp Makefile
	@echo "Compiling $<"
	@$(CXX) $(CXXFLAGS) $(CFLAGS) $(INCLUDES) -c $< -o $@ -MD -MP

$(PLUGIN): $(OBJECTS) Makefile
	@echo "Linking   $@"
	@$(CXX) $(LINKLINE)
	@echo "Stripping $@"
	@$(STRIP) $@

test: test-plugin test-verify

test-plugin: plugin-test.cpp $(PLUGIN) Makefile
	@echo "Compiling plugin-test"
	@$(CXX) $(CXXFLAGS) $(CFLAGS) plugin-test.cpp -ldl -o plugin-test
	@echo "Running   plugin-test..."
	@$(PROJECTPATH)/plugin-test $(PLUGINPATH)

test-verify: verify-test.cpp $(OBJECTS) Makefile
	@echo "Compiling verify-test"
	@$(CXX) $(CXXFLAGS) $(CFLAGS) $(LDFLAGS) $(OBJECTS) verify-test.cpp $(LIBS) -o verify-test
	@echo "Running   verify-test..."
	@$(PROJECTPATH)/verify-test
