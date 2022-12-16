//
// Created by Markus Pilman on 10/15/22.
//

#ifndef FLATBUFFER_ERROR_H
#define FLATBUFFER_ERROR_H

#include <string>

namespace flowserializer {

class Error : std::exception {
	std::string msg;

public:
	explicit Error(std::string msg);
	[[nodiscard]] const char* what() const noexcept override;
};

class InternalError : public Error {
public:
	InternalError() : Error("Internal Error") {}
};

template <class B>
B assertTrue(B b) {
	if (!b) {
		throw InternalError();
	}
	return b;
}

} // namespace flowserializer

#endif // FLATBUFFER_ERROR_H
