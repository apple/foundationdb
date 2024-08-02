#include "fdbmonitor.h"

#include <cassert>

namespace fdbmonitor {
namespace tests {

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

int testPathOps() {
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
	return errors;
}
} // namespace tests
} // namespace fdbmonitor

int main(int argc, char** argv) {
	using namespace fdbmonitor::tests;

	assert(testPathOps() /* errors */ == 0);
}