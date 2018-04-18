// Apple Proprietary and Confidential Information

#include "boost/config.hpp"

#include "FDBLibTLSPlugin.h"
#include "FDBLibTLSPolicy.h"

#include <string.h>

FDBLibTLSPlugin::FDBLibTLSPlugin() {
	// tls_init is not currently thread safe - caller's responsibility.
	rc = tls_init();
}

FDBLibTLSPlugin::~FDBLibTLSPlugin() {
}

ITLSPolicy *FDBLibTLSPlugin::create_policy(ITLSLogFunc logf) {
	if (rc < 0) {
		// Log the failure from tls_init during our constructor.
		logf("FDBLibTLSInitError", NULL, true, "LibTLSErrorMessage", "failed to initialize libtls", NULL);
		return NULL;
	}
	return new FDBLibTLSPolicy(Reference<FDBLibTLSPlugin>::addRef(this), logf);
}

extern "C" BOOST_SYMBOL_EXPORT void *get_plugin(const char *plugin_type_name_and_version) {
	if (strcmp(plugin_type_name_and_version, FDBLibTLSPlugin::get_plugin_type_name_and_version()) == 0) {
		return new FDBLibTLSPlugin;
	}
	return NULL;
}
