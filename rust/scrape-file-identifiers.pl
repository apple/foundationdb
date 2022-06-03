#!/usr/bin/perl -w
use strict;

use Cwd qw/abs_path/;

my $script_dir = `dirname $0`;
chomp $script_dir;
chdir "$script_dir/.." || die;
#my ($real_path) = abs_path($0) =~ m/(.*)/[^/]+/i;
#chdir $real_path;

my @lines = `git grep -B1 FileIdentifier\\\ file_identifier\\\ =\\\ `;

print "// generated with ./scrape-file-identifiers.pl > src/flow/file_identifier_table.rs\n";
print "// you may need to edit the FDB C++ code to remove any ?s in the output.\n";
print "pub fn file_identifier_table() -> std::collections::HashMap<&'static str, u32> {\n   vec![\n";

my $struct;
for my $line (@lines) {
    chomp $line;
    if ($line =~ /^--$/) { 
        $struct = undef;
    } elsif ($line =~ /struct\s+(\S+)/) {
        $struct = $1;
    } elsif ($line =~ /file_identifier = (\d+)/) {
        print "      (\"$struct\", $1),\n";
    } elsif ($line =~ /file_identifier = /) {
        # skip
    } else {
        warn "??? $line\n";
    }
}
print "   ].into_iter().collect()\n}\n";