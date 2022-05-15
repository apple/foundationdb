from conans import ConanFile, CMake

class FoundationDB(ConanFile):
    name = "FoundationDB"
    url = "https://www.foundationdb.org"
    settings = "os", "compiler", "build_type", "arch"
    requires = "boost/1.78.0", "openssl/1.1.1n", "toml11/3.7.1", "jemalloc/5.3.0", "aws-sdk-cpp/1.9.234", "benchmark/1.6.1", "msgpack/3.3.0", "fmt/8.1.1"
    generators = "cmake_find_package", "cmake_paths"
    options = {
        "java": [True, False]
    }
    default_options = {
        "java": True,
        "jemalloc:enable_cxx": False,
        "jemalloc:shared": False,
        "aws-sdk-cpp:s3": True
    }

    def configure(self):
        if self.settings.os == 'Macos' and self.settings.arch == 'armv8':
            self.options.java = False

    def requirements(self):
        if self.options.java:
            self.requires("openjdk/16.0.1")

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
