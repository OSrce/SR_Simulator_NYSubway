#!/usr/bin/perl 
#use strict;

use Time::Piece;
my ($year, $month, $day) = qw(2002 02 23);

my $t = Time::Piece->strptime("$year/$month/$day", "%Y/%m/%d");

print "day of week is " . $t->day_of_week . "\n";
print "day of week is " . $t->day . "\n";
