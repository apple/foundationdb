#
# packages.mk
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

TARGETS += packages
CLEAN_TARGETS += packages_clean

PACKAGE_BINARIES = fdbcli fdbserver fdbbackup fdbmonitor fdbrestore fdbdr dr_agent backup_agent
PACKAGE_CONTENTS := $(addprefix bin/, $(PACKAGE_BINARIES)) $(addprefix bin/, $(addsuffix .debug, $(PACKAGE_BINARIES))) lib/libfdb_c.$(DLEXT) bindings/python/fdb/fdboptions.py bindings/c/foundationdb/fdb_c_options.g.h

packages: TGZ FDBSERVERAPI

TGZ: $(PACKAGE_CONTENTS) versions.target lib/libfdb_java.$(DLEXT)
	@echo "Archiving      tgz"
	@mkdir -p packages
	@rm -f packages/FoundationDB-$(PLATFORM)-*.tar.gz
	@bash -c "tar -czf packages/FoundationDB-$(PLATFORM)-$(VERSION)-$(PKGRELEASE).tar.gz bin/{fdbmonitor{,.debug},fdbcli{,.debug},fdbserver{,.debug},fdbbackup{,.debug},fdbdr{,.debug},fdbrestore{,.debug},dr_agent{,.debug},coverage.{fdbclient,fdbserver,fdbrpc,flow}.xml} lib/libfdb_c.$(DLEXT){,-debug} lib/libfdb_java.$(DLEXT)* bindings/python/fdb/*.py bindings/c/*.h"

packages_clean:
	@echo "Cleaning       packages"
	@rm -f packages/FoundationDB-$(PLATFORM)-*.tar.gz packages/fdb-tests-$(VERSION).tar.gz packages/fdb-headers-$(VERSION).tar.gz packages/fdb-bindings-$(VERSION).tar.gz packages/fdb-server-$(VERSION)-$(PLATFORM).tar.gz

packages/fdb-server-$(VERSION)-$(PLATFORM).tar.gz: bin/fdbserver bin/fdbcli lib/libfdb_c.$(DLEXT)
	@echo "Packaging      fdb server api"
	@rm -rf packages/fdbserverapi
	@mkdir -p packages/fdbserverapi/bin packages/fdbserverapi/lib
	@cp bin/fdbserver bin/fdbcli packages/fdbserverapi/bin/
	@cp lib/libfdb_c.$(DLEXT) packages/fdbserverapi/lib/
	@tar czf packages/fdb-server-$(VERSION)-$(PLATFORM).tar.gz -C packages/fdbserverapi/ .
	@rm -rf packages/fdbserverapi

FDBSERVERAPI: packages/fdb-server-$(VERSION)-$(PLATFORM).tar.gz

FDBSERVERAPI_clean:
	@echo "Cleaning       fdb server api"
	@rm -rf packages/fdb-server-$(VERSION)-$(PLATFORM).tar.gz packages/fdbserverapi

ifeq ($(PLATFORM),linux)
  DEB: packages/foundationdb-clients_$(VERSION)-$(PKGRELEASE)_amd64.deb packages/foundationdb-server_$(VERSION)-$(PKGRELEASE)_amd64.deb

  DEB_clean:
	@echo "Cleaning       deb"
	@rm -f packages/foundationdb-server_*.deb packages/foundationdb-clients_*.deb

  DEB_FILES := $(addprefix packaging/deb/,builddebs.sh foundationdb-clients.control.in foundationdb-init foundationdb-server.control.in DEBIAN-foundationdb-clients/postinst $(addprefix DEBIAN-foundationdb-server/,conffiles postinst postrm preinst prerm))

  packages/foundationdb-server_%.deb packages/foundationdb-clients_%.deb: $(PACKAGE_CONTENTS) versions.target $(DEB_FILES)
	@echo "Packaging      deb"
	@mkdir -p packages
	@rm -f packages/foundationdb-server_*.deb packages/foundationdb-clients_*.deb
	@mkdir -p packaging/deb/DEBIAN-foundationdb-server packaging/deb/DEBIAN-foundationdb-clients
	@for i in server clients; do \
		m4 -DVERSION=$(VERSION) -DRELEASE=$(PKGRELEASE) packaging/deb/foundationdb-$$i.control.in > packaging/deb/DEBIAN-foundationdb-$$i/control; \
	done
	@packaging/deb/builddebs.sh
	@rm packaging/deb/DEBIAN-*/control

  RPM: packages/foundationdb-server-$(VERSION)-$(PKGRELEASE).el6.x86_64.rpm packages/foundationdb-clients-$(VERSION)-$(PKGRELEASE).el6.x86_64.rpm packages/foundationdb-server-$(VERSION)-$(PKGRELEASE).el7.x86_64.rpm packages/foundationdb-clients-$(VERSION)-$(PKGRELEASE).el7.x86_64.rpm

  RPM_clean:
	@echo "Cleaning       rpm"
	@rm -f packages/foundationdb-server-*.rpm packages/foundationdb-clients-*.rpm

  RPM_FILES := $(addprefix packaging/rpm/,buildrpms.sh foundationdb-init foundationdb.service foundationdb.spec.in)

  JAVA_RELEASE: fdb_java_release

  JAVA_RELEASE_clean: fdb_java_release_clean

  FDBTESTS:
	@echo "Archiving      fdbtests"
	@mkdir -p packages
	@rm -f packages/fdb-tests-$(VERSION).tar.gz
	@bash -c "tar -czf packages/fdb-tests-$(VERSION).tar.gz -C tests ."

  FDBTESTS_clean:
	@echo "Cleaning       fdbtests"
	@rm -f packages/fdb-tests-$(VERSION).tgz

  FDBBINDINGS: bindings
	@echo "Archiving      fdbbindings"
	@mkdir -p packages
	@rm -f packages/fdb-bindings-$(VERSION).tar.gz
	@bash -c "tar -czf packages/fdb-bindings-$(VERSION).tar.gz -C bindings ."

  FDBBINDINGS_clean:
	@echo "Cleaning       fdbbindings"
	@rm -f packages/fdb-bindings-$(VERSION).tgz

  FDBHEADERS: bindings/python/fdb/fdboptions.py bindings/c/foundationdb/fdb_c_options.g.h fdbclient/vexillographer/fdb.options
	@echo "Archiving      fdbheaders"
	@mkdir -p packages
	@rm -f packages/fdb-headers-$(VERSION).tar.gz
	@bash -c "tar -czf packages/fdb-headers-$(VERSION).tar.gz -C $(shell pwd)/bindings/c/foundationdb fdb_c.h -C $(shell pwd)/bindings/c/foundationdb fdb_c_options.g.h -C $(shell pwd)/fdbclient/vexillographer fdb.options"

  FDBHEADERS_clean:
	@echo "Cleaning       fdbheaders"
	@rm -f packages/fdb-headers-$(VERSION).tgz

  packages/foundationdb-server-%.el6.x86_64.rpm packages/foundationdb-clients-%.el6.x86_64.rpm packages/foundationdb-server-%.el7.x86_64.rpm packages/foundationdb-clients-%.el7.x86_64.rpm: $(PACKAGE_CONTENTS) versions.target $(RPM_FILES)

  packages/foundationdb-server-%.el6.x86_64.rpm packages/foundationdb-clients-%.el6.x86_64.rpm packages/foundationdb-server-%.el7.x86_64.rpm packages/foundationdb-clients-%.el7.x86_64.rpm: $(PACKAGE_CONTENTS) versions.target $(RPM_FILES)
	@echo "Packaging      rpm"
	@mkdir -p packages
	@rm -f packages/foundationdb-server-*.rpm packages/foundationdb-clients-*.rpm
	@packaging/rpm/buildrpms.sh $(VERSION) $(PKGRELEASE)

  FDBTLS: bin/fdb-libressl-plugin.$(DLEXT)

  packages: DEB RPM JAVA_RELEASE FDBTESTS FDBHEADERS FDBTLS

  packages_clean: DEB_clean RPM_clean JAVA_RELEASE_clean FDBHEADERS_clean

endif

ifeq ($(PLATFORM),osx)
  ifeq ($(RELEASE),true)
    PKGFILE := packages/FoundationDB-$(VERSION).pkg
  else
    PKGFILE := packages/FoundationDB-$(VERSION)-PRERELEASE.pkg
  endif

  PKG: $(PACKAGE_CONTENTS) versions.target
	@mkdir -p packages
	@rm -f packages/*.pkg
	@packaging/osx/buildpkg.sh $(PKGFILE) $(VERSION) $(PKGRELEASE)

  packages: PKG

  packages_clean: packages_osx_clean

  packages_osx_clean:
	@rm -f packages/*.pkg
endif
