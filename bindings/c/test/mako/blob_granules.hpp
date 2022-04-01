#ifndef MAKO_BLOB_GRANULES_HPP
#define MAKO_BLOB_GRANULES_HPP

#include <cstdint>
#include <memory>
#include <fdb.hpp>

namespace mako::blob_granules::local_file {

constexpr const int MAX_BG_IDS = 1000;

// TODO: could always abstract this into something more generically usable by something other than mako.
// But outside of testing there are likely few use cases for local granules
struct UserContext {
	char const* bgFilePath;
	int nextId;
	std::unique_ptr<uint8_t*[]> dataByIdMem;
	uint8_t** dataById;

	UserContext(char const* filePath)
	  : bgFilePath(filePath), nextId(0), dataByIdMem(new uint8_t*[MAX_BG_IDS]()), dataById(dataByIdMem.get()) {}

	void clear() { dataByIdMem.reset(); }
};

fdb::native::FDBReadBlobGranuleContext createApiContext(UserContext& ctx, bool materialize_files);

} // namespace mako::blob_granules::local_file

#endif /*MAKO_BLOB_GRANULES_HPP*/
