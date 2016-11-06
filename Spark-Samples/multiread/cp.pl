#!/usr/bin/perl
use 5.14.0;
use warnings FATAL => 'all';

my $data = `sbt "show fullClasspath"`;
($data =~ /List\((.*)\)/) or die;
my $list = $1;

$list =~ s/,\s*/:/g;
$list =~ s/Attributed\((.*?)\)/$1/g;

say $list;
