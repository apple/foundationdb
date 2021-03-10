#include <cstdint>
#include <unordered_map>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <stack>
#include <memory>
#include <sstream>
#include <algorithm>

namespace {

void usage(const char* execName, std::ostream& out) {
	out << "USAGE: " << execName << " [OPTIONS] [--] file [file]..." << std::endl;
	out << '\t' << "-h|--help: print this help" << std::endl;
}

struct Error {
	const char* msg = "";
	bool isFatal = false;
	const char* what() const { return msg; }
};

struct Actor {
	template <class Str>
	explicit Actor(std::unordered_map<std::string, unsigned long>& results, unsigned long id, Str&& name)
	  : results(results), id(id), name(std::forward<Str>(name)) {}
	Actor(const Actor&) = delete;
	~Actor() { collect(); }
	std::unordered_map<std::string, unsigned long>& results;
	unsigned long id;
	std::string name;
	std::deque<std::string> stack;
	unsigned long runTime = 0;
	unsigned long lastStart = 0;
	void enter(unsigned long time) { lastStart = time; }
	void exit(unsigned long time) { runTime += time - lastStart; }
	void collect() {
		std::stringstream ss;
		for (auto i = stack.begin(); i != stack.end();) {
			int num = 0;
			auto name = *i;
			for (; i != stack.end() && *i == name; ++i) {
				++num;
			}
			ss << name;
			if (num > 1) {
				ss << " (" << num << ')';
			}
			ss << ';';
		}
		ss << name;
		auto myStack = ss.str();
		results[myStack] += std::max(1ul, runTime);
	}
};

class Traces {
	constexpr static int OP_CREATE = 0;
	constexpr static int OP_DESTROY = 1;
	constexpr static int OP_ENTER = 2;
	constexpr static int OP_EXIT = 3;
	std::stack<std::shared_ptr<Actor>> currentStack;
	std::unordered_map<unsigned long, std::shared_ptr<Actor>> actors;
	std::unordered_map<std::string, unsigned long> results;

	std::vector<std::string> split(const std::string& str, char delim) {
		std::vector<std::string> res;
		std::string::size_type pos = 0;
		while (pos < str.size()) {
			auto e = str.find(delim, pos);
			if (e == std::string::npos) {
				res.emplace_back(str.substr(pos));
				break;
			}
			res.emplace_back(str.substr(pos, e - pos));
			pos = e + 1;
		}
		return res;
	}

public:
	void print(std::ostream& out) const {
		for (const auto& r : results) {
			out << r.first << ' ' << r.second << std::endl;
		}
	}

	void operator()(std::istream& in) {
		int lineNo = 0;
		std::string line;
		while (std::getline(in, line)) {
			++lineNo;
			if (line.empty()) {
				continue;
			}
			auto v = split(line, ';');
			if (v.size() != 4) {
				Error e;
				e.msg = "Could not parse line";
				throw e;
			}
			unsigned long timestamp = std::stoul(v[0]);
			int op = std::stoi(v[1]);
			const auto& name = v[2];
			unsigned long id = std::stoul(v[3]);
			if (op == OP_CREATE) {
				actors[id] = std::make_shared<Actor>(results, id, name);
				auto& actor = actors[id];
				if (!currentStack.empty()) {
					actor->stack = currentStack.top()->stack;
					actor->stack.push_back(currentStack.top()->name);
				}
			} else if (op == OP_DESTROY) {
				if (actors.count(id)) {
					actors.erase(id);
				}
			} else if (op == OP_ENTER) {
				if (actors.count(id) == 0) {
					actors[id] = std::make_shared<Actor>(results, id, name);
				}
				currentStack.push(actors[id]);
				actors[id]->enter(timestamp);
			} else if (op == OP_EXIT) {
				if (!currentStack.empty()) {
					if (currentStack.top()->id != id) {
						std::cerr << "WARNING: Unbalanced stack at line " << lineNo << std::endl;
					} else {
						currentStack.top()->exit(timestamp);
						currentStack.pop();
					}
				}
			}
		}
		std::cout << "DONE" << std::endl;
		while (!currentStack.empty()) {
			currentStack.pop();
		}
		actors.clear();
	}
};

} // namespace

int main(int argc, char* argv[]) {
	std::vector<std::string> files;
	bool endOfArgs = false;
	for (int i = 1; i < argc; ++i) {
		std::string arg(argv[i]);
		if (endOfArgs) {
			files.emplace_back(arg);
		} else if (arg == "--") {
			endOfArgs = true;
		} else if (arg == "-h" || arg == "--") {
			usage(argv[0], std::cout);
			return 0;
		} else if (arg[0] != '-') {
			files.emplace_back(arg);
		} else {
			std::cerr << "Unknown argument \"" << arg << "\"" << std::endl;
			usage(argv[0], std::cerr);
			return 1;
		}
	}
	if (files.empty()) {
		std::cerr << "ERROR: No file" << std::endl;
	}
	Traces traces;
	for (const auto& file : files) {
		std::fstream in(file.c_str(), std::ios_base::in);
		if (!in) {
			std::cerr << "Error: can't open file: " << file << std::endl;
			return 1;
		}
		try {
			traces(in);
		} catch (Error& e) {
			std::cerr << (e.isFatal ? "FATAL: " : "ERROR: ") << e.what() << std::endl;
			if (e.isFatal) {
				return 1;
			}
		}
	}
	traces.print(std::cout);
	return 0;
}
