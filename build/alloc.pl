#!/usr/bin/env perl
use strict;

# Processes the stdout produced by defining ALLOC_INSTRUMENTATION_STDOUT in FastAlloc.h

# Periodically show the top N stacks by size.
# The full list will be output at the end.
my $topN = 15;
my $everyN = 1000000;
my %allocs;

sub print_stacks
{
  my $n = shift;
  my %counts;
  my %sizes;
  while(my ($id, $info) = each %allocs)
  {
    my ($size, $stack) = @$info;
    $counts{$stack} += 1;
    $sizes{$stack} += $size;
  }

  my @stacks = keys %counts;
  # Sort stack traces by size in reverse order
  @stacks = sort {$sizes{$a} <=> $sizes{$b}} @stacks;
  @stacks = @stacks[-$topN..-1] if $n;
  map {print "bytes=$sizes{$_}\tcount=$counts{$_}\t$_\n" if $_} @stacks;
  print '-' x 60, "\n";
}

my $lines = 0;

while(<>)
{
  chomp;
  print_stacks($topN), $lines = 0 if $lines++ == $everyN;
  my ($type, $id, $size, $trace) = split /\t/;
  unless($type =~ /(Dealloc|Alloc)/) {
    print "$_\n";
    next;
  }
  $type = $1;
  if($type eq "Alloc")
  { 
    $allocs{$id} = [$size, $trace];
  }
  elsif($type eq "Dealloc")
  {
    #warn "not yet created or double destroyed: $id $trace" if not exists $allocs{$id};
    delete $allocs{$id};
  }
}

print_stacks();
