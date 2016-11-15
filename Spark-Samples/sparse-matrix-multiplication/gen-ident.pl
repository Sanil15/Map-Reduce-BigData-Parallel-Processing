#!/usr/bin/perl
use 5.16.0;
use warnings FATAL => 'all';

my $nn = shift || 100;

for (my $ii = 0; $ii < $nn; ++$ii) {
    say "$ii, $ii, 1.0";
}
