#
# relativelink.py
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

from sphinx.addnodes import toctree

# This extension cruelly monkey patches sphinx.environment.BuildEnvironment so
# that toctree entries can contain relative internal links, using the syntax
# Name <relative://relative/path>
# This is translated into an href="relative/path"

# Relative links already work fine outside the toctree:

# Name <relative/path>_

def setup(app):
    import sphinx.environment
    from docutils import nodes

    old_resolve = sphinx.environment.BuildEnvironment.resolve_toctree
    def resolve_toctree(self, docname, builder, toctree, prune=True, maxdepth=0,
                            titles_only=False, collapse=False, includehidden=False):
        result = old_resolve(self, docname, builder, toctree, prune=True, maxdepth=0,
                            titles_only=False, collapse=False, includehidden=False)
        if result == None:
            return result

        for node in result.traverse( nodes.reference ):
            if not node['internal'] and node['refuri'].startswith("relative://"):
                node['refuri'] = node['refuri'][len("relative://"):]
        return result
    sphinx.environment.BuildEnvironment.resolve_toctree = resolve_toctree
