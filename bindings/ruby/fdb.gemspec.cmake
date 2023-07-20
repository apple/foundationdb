# -*- mode: ruby; -*-

Gem::Specification.new do |s|
  s.name = 'fdb'
  s.version = '${FDB_VERSION}'
  s.date = Time.new.strftime '%Y-%m-%d'
  s.summary = "Ruby bindings for the FoundationDB database"
  s.description = <<-EOF
Ruby bindings for the FoundationDB database.

Complete documentation of the FoundationDB Ruby API can be found at:
https://apple.github.io/foundationdb/api-ruby.html.
EOF
  s.authors = ["FoundationDB"]
  s.email = 'fdb-dist@apple.com'
  s.files = ["${CMAKE_SOURCE_DIR}/LICENSE", "${CMAKE_CURRENT_SOURCE_DIR}/lib/fdb.rb", "${CMAKE_CURRENT_SOURCE_DIR}/lib/fdbdirectory.rb", "${CMAKE_CURRENT_SOURCE_DIR}/lib/fdbimpl.rb", "${CMAKE_CURRENT_SOURCE_DIR}/lib/fdblocality.rb", "${CMAKE_CURRENT_SOURCE_DIR}/lib/fdboptions.rb", "${CMAKE_CURRENT_SOURCE_DIR}/lib/fdbsubspace.rb", "${CMAKE_CURRENT_SOURCE_DIR}/lib/fdbtuple.rb", "${CMAKE_CURRENT_SOURCE_DIR}/lib/fdbimpl_v609.rb"]
  s.homepage = 'https://www.foundationdb.org'
  s.license = 'Apache-2.0'
  s.add_dependency('ffi', '~> 1.1', '>= 1.1.5')
  s.required_ruby_version = '>= 1.9.3'
  s.requirements << 'These bindings require the FoundationDB client. The client can be obtained from https://www.foundationdb.org/download/.'
end
