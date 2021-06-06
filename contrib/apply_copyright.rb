#! /usr/bin/env ruby

$copyright = <<EOF
/*
 * FILENAME
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-YEAR Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

EOF

def main
  if ARGV.size == 0
    puts "Usage: apply_copyright.rb [file name] [file name] ..."
    exit 1
  end

  ARGV.each do |path|
    ext_name = File.extname(path)
    next unless ['.cpp', '.h', '.c'].include?(ext_name)

    file_name = File.basename(path)

    content = File.readlines(path)
    if content[0] != "/*\n"
      # No copyright, insert one
      content.unshift($copyright.gsub(/FILENAME/, file_name).gsub(/YEAR/, Time.new.year.to_s))
      File.open(path, 'w') { |stream| stream.puts(content) }
    elsif content[1] != " * #{file_name}\n"
      # Update the file name
      content[1] = " * #{file_name}\n"
      File.open(path, 'w') { |stream| stream.puts(content) }
    end
  end
end

main if $0 == __FILE__
