#!/usr/bin/env python
# -*- coding: utf-8 -*-

from conans import ConanFile, tools
import os

class SparseppConan(ConanFile):
    name = "parallel_hashmap"
    version = "1.27"
    description = "A header-only, very fast and memory-friendly hash map"
    
    # Indicates License type of the packaged library
    license = "https://github.com/greg7mdp/parallel-hashmap/blob/master/LICENSE"
    
    # Packages the license for the conanfile.py
    exports = ["LICENSE"]
    
    # Custom attributes for Bincrafters recipe conventions
    source_subfolder = "source_subfolder"
    
    def source(self):
        source_url = "https://github.com/greg7mdp/parallel-hashmap"
        tools.get("{0}/archive/{1}.tar.gz".format(source_url, self.version))
        extracted_dir = self.name + "-" + self.version

        #Rename to "source_folder" is a convention to simplify later steps
        os.rename(extracted_dir, self.source_subfolder)


    def package(self):
        include_folder = os.path.join(self.source_subfolder, "parallel_hashmap")
        self.copy(pattern="LICENSE")
        self.copy(pattern="*", dst="include/parallel_hashmap", src=include_folder)

    def package_id(self):
        self.info.header_only()
