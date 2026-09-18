#include "fdbmonitor.h"

#include <cassert>
#include <string>
#include <functional>
#include <limits>

namespace fdbmonitor {
namespace tests {

void assert_msg(const bool cond, const std::string& msg) {
	if (!cond) {
		printf("%s\n", msg.c_str());
		std::abort();
	}
}

int testPathFunction(const char* name, std::function<std::string(std::string)> fun, std::string a, std::string b) {
	std::string o = fun(a);
	bool r = b == o;
	printf("%s: %s(%s) = %s expected %s\n", r ? "PASS" : "FAIL", name, a.c_str(), o.c_str(), b.c_str());
	return r ? 0 : 1;
}

int testPathFunction2(const char* name,
                      std::function<std::string(std::string, bool)> fun,
                      std::string a,
                      bool x,
                      std::string b) {
	std::string o = fun(a, x);
	bool r = b == o;
	printf("%s: %s(%s, %d) => %s expected %s\n", r ? "PASS" : "FAIL", name, a.c_str(), x, o.c_str(), b.c_str());
	return r ? 0 : 1;
}

void testPathOps() {
	int errors = 0;

	errors += testPathFunction("popPath", popPath, "a", "");
	errors += testPathFunction("popPath", popPath, "a/", "");
	errors += testPathFunction("popPath", popPath, "a///", "");
	errors += testPathFunction("popPath", popPath, "a///..", "a/");
	errors += testPathFunction("popPath", popPath, "a///../", "a/");
	errors += testPathFunction("popPath", popPath, "a///..//", "a/");
	errors += testPathFunction("popPath", popPath, "/", "/");
	errors += testPathFunction("popPath", popPath, "/a", "/");
	errors += testPathFunction("popPath", popPath, "/a/b", "/a/");
	errors += testPathFunction("popPath", popPath, "/a/b/", "/a/");
	errors += testPathFunction("popPath", popPath, "/a/b/..", "/a/b/");

	errors += testPathFunction("cleanPath", cleanPath, "/", "/");
	errors += testPathFunction("cleanPath", cleanPath, "..", "..");
	errors += testPathFunction("cleanPath", cleanPath, "../.././", "../..");
	errors += testPathFunction("cleanPath", cleanPath, "///.///", "/");
	errors += testPathFunction("cleanPath", cleanPath, "/a/b/.././../c/./././////./d/..//", "/c");
	errors += testPathFunction("cleanPath", cleanPath, "a/b/.././../c/./././////./d/..//", "c");
	errors += testPathFunction("cleanPath", cleanPath, "a/b/.././../c/./././////./d/..//..", ".");
	errors += testPathFunction("cleanPath", cleanPath, "a/b/.././../c/./././////./d/..//../..", "..");
	errors += testPathFunction("cleanPath", cleanPath, "../a/b/..//", "../a");
	errors += testPathFunction("cleanPath", cleanPath, "/..", "/");
	errors += testPathFunction("cleanPath", cleanPath, "/../foo/bar///", "/foo/bar");
	errors += testPathFunction("cleanPath", cleanPath, "/a/b/../.././../", "/");
	errors += testPathFunction("cleanPath", cleanPath, ".", ".");

	mkdir("simfdb/backups/one/two/three");
	std::string cwd = abspath(".", true);

	// Create some symlinks and test resolution (or non-resolution) of them
	[[maybe_unused]] int rc;
	// Ignoring return codes, if symlinks fail tests below will fail
	rc = unlink("simfdb/backups/four");
	rc = unlink("simfdb/backups/five");
	rc = symlink("one/two", "simfdb/backups/four") == 0 ? 0 : 1;
	rc = symlink("../backups/four", "simfdb/backups/five") ? 0 : 1;

	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../two", true, joinPath(cwd, "simfdb/backups/one/two"));
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../three", true, joinPath(cwd, "simfdb/backups/one/three"));
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../three/../four", true, joinPath(cwd, "simfdb/backups/one/four"));

	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/five/../two", true, joinPath(cwd, "simfdb/backups/one/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/five/../three", true, joinPath(cwd, "simfdb/backups/one/"));

	errors += testPathFunction2("abspath", abspath, "/", false, "/");
	errors += testPathFunction2("abspath", abspath, "/foo//bar//baz/.././", false, "/foo/bar");
	errors += testPathFunction2("abspath", abspath, "/", true, "/");
	errors += testPathFunction2("abspath", abspath, "", true, "");
	errors += testPathFunction2("abspath", abspath, ".", true, cwd);
	errors += testPathFunction2("abspath", abspath, "/a", true, "/a");
	errors += testPathFunction2("abspath", abspath, "one/two/three/four", false, joinPath(cwd, "one/two/three/four"));
	errors += testPathFunction2("abspath", abspath, "one/two/three/./four", false, joinPath(cwd, "one/two/three/four"));
	errors += testPathFunction2("abspath", abspath, "one/two/three/./four/..", false, joinPath(cwd, "one/two/three"));
	errors +=
	    testPathFunction2("abspath", abspath, "one/./two/../three/./four", false, joinPath(cwd, "one/three/four"));
	errors +=
	    testPathFunction2("abspath", abspath, "simfdb/backups/four/../two", false, joinPath(cwd, "simfdb/backups/two"));
	errors +=
	    testPathFunction2("abspath", abspath, "simfdb/backups/five/../two", false, joinPath(cwd, "simfdb/backups/two"));
	errors += testPathFunction2("abspath", abspath, "foo/./../foo2/./bar//", false, joinPath(cwd, "foo2/bar"));
	errors += testPathFunction2("abspath", abspath, "foo/./../foo2/./bar//", true, joinPath(cwd, "foo2/bar"));

	errors += testPathFunction2("parentDirectory", parentDirectory, "", true, "");
	errors += testPathFunction2("parentDirectory", parentDirectory, "/", true, "/");
	errors += testPathFunction2("parentDirectory", parentDirectory, "/a", true, "/");
	errors += testPathFunction2("parentDirectory", parentDirectory, ".", false, cleanPath(joinPath(cwd, "..")) + "/");
	errors += testPathFunction2("parentDirectory", parentDirectory, "./foo", false, cleanPath(cwd) + "/");
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/two/three/four", false, joinPath(cwd, "one/two/three/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/two/three/./four", false, joinPath(cwd, "one/two/three/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/two/three/./four/..", false, joinPath(cwd, "one/two/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/./two/../three/./four", false, joinPath(cwd, "one/three/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/four/../two", false, joinPath(cwd, "simfdb/backups/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/five/../two", false, joinPath(cwd, "simfdb/backups/"));
	errors +=
	    testPathFunction2("parentDirectory", parentDirectory, "foo/./../foo2/./bar//", false, joinPath(cwd, "foo2/"));
	errors +=
	    testPathFunction2("parentDirectory", parentDirectory, "foo/./../foo2/./bar//", true, joinPath(cwd, "foo2/"));

	printf("%d errors.\n", errors);
	assert(errors == 0);
}

void testEnvVarUtils() {
	// Ensure key-value extraction works
	const std::pair<std::string, std::string> keyValuePair1{ "FOO", "BAR" };
	assert(keyValuePair1 == EnvVarUtils::extractKeyAndValue("FOO=BAR"));
	const std::pair<std::string, std::string> keyValuePair2{ "x", "y" };
	assert(keyValuePair2 == EnvVarUtils::extractKeyAndValue("x=y"));
	const std::pair<std::string, std::string> keyValuePair3{ "MALLOC_CONF",
		                                                     "prof:true,lg_prof_interval:30,prof_prefix:jeprof.out" };
	assert(keyValuePair3 ==
	       EnvVarUtils::extractKeyAndValue("MALLOC_CONF=prof:true,lg_prof_interval:30,prof_prefix:jeprof.out"));

	// Ensure key-value validation passes for good inputs
	assert(EnvVarUtils::keyValueValid("FOO=BAR", "FOO=BAR"));
	assert(EnvVarUtils::keyValueValid("x=y", "FOO=BAR x=y"));
	assert(EnvVarUtils::keyValueValid("MALLOC_CONF=prof:true,lg_prof_interval:30,prof_prefix:jeprof.out",
	                                  "MALLOC_CONF=prof:true,lg_prof_interval:30,prof_prefix:jeprof.out"));
	assert(EnvVarUtils::keyValueValid("MALLOC_CONF=prof:true,lg_prof_interval:30,prof_prefix:jeprof.out",
	                                  "MALLOC_CONF=prof:true,lg_prof_interval:30,prof_prefix:jeprof.out FOO=BAR"));

	// Ensure key-value validation fails for bad inputs
	assert_msg(!EnvVarUtils::keyValueValid("", "FOO=BAR ="), "Key-Value can not be empty");
	assert_msg(!EnvVarUtils::keyValueValid("FOO==BAR", "FOO==BAR"), "Only one equal sign allowed");
	assert_msg(!EnvVarUtils::keyValueValid("BAZ=", "FOO=BAR BAZ="), "Value must be non-empty");
	assert_msg(!EnvVarUtils::keyValueValid("=BAZ", "FOO=BAR =BAZ"), "Key must be non-empty");
}

void testRssParsing() {
	uint64_t rss = 123;
	assert_msg(parseRss("100 20 3 4 5 6 7\n", 4096, rss) && rss == 81920, "Resident pages must convert to bytes");
	assert_msg(parseRss(" \t100 0\n", 4096, rss) && rss == 0, "Zero resident pages must be valid");
	const char bounded[] = { '1', '0', '0', ' ', '2', '0' };
	assert_msg(parseRss(std::string_view(bounded, 5), 4096, rss) && rss == 8192, "Parsing must honor the input length");
	const uint64_t maxBytes = std::numeric_limits<uint64_t>::max();
	assert_msg(parseRss("100 " + std::to_string(maxBytes), 1, rss) && rss == maxBytes,
	           "The largest byte count must be valid");
	const uint64_t maxPages = maxBytes / 4096;
	assert_msg(parseRss("100 " + std::to_string(maxPages), 4096, rss) && rss == maxPages * 4096,
	           "The largest whole-page byte count must be valid");
	for (const char* invalid : { "",
	                             " \t",
	                             "100",
	                             "100 ",
	                             "x 20",
	                             "100 x",
	                             "100 2x",
	                             "-1 20",
	                             "100 -1",
	                             "18446744073709551616 20",
	                             "100 18446744073709551616" }) {
		rss = 123;
		assert_msg(!parseRss(invalid, 4096, rss) && rss == 123, "Invalid statm must leave RSS unchanged");
	}
	rss = 123;
	assert_msg(!parseRss("100 " + std::to_string(maxPages + 1), 4096, rss) && rss == 123,
	           "Resident byte-count overflow must be rejected");
	assert_msg(!parseRss("100 20", 0, rss) && rss == 123, "Zero page size must be rejected");
	assert_msg(!parseRss("100 20", -1, rss) && rss == 123, "Failed page-size lookup must be rejected");
}

} // namespace tests
} // namespace fdbmonitor

int main(int argc, char** argv) {
	using namespace fdbmonitor::tests;

	testPathOps();
	testEnvVarUtils();
	testRssParsing();
}
