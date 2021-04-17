

#include "fdbserver/IConfigurationConsumer.h"

class SimpleConfigurationConsumerImpl {
public:
	SimpleConfigurationConsumerImpl(ClusterConnectionString const& ccs) {
		// TODO: Implement
	}

	Future<Void> serve(ConfigurationConsumerInterface& cci) {
		// TODO: Implement
		return Void();
	}
};

SimpleConfigurationConsumer::SimpleConfigurationConsumer(ClusterConnectionString const& ccs)
  : impl(std::make_unique<SimpleConfigurationConsumerImpl>(ccs)) {}

SimpleConfigurationConsumer::~SimpleConfigurationConsumer() = default;

Future<Void> SimpleConfigurationConsumer::serve(ConfigurationConsumerInterface& cci) {
	return impl->serve(cci);
}
