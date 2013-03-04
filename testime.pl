#!/usr/bin/perl

# load module
use DBI;
use POSIX qw/strftime/;


@tokens = split(/:/, "10:02:11");
print "@tokens[0]\n";
print "@tokens[1]\n";
print "@tokens[2]\n";
