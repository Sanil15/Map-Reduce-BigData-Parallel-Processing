#!/usr/bin/perl
use 5.16.0;
use warnings FATAL => 'all';

use Test::Simple tests => 2;

# Check sampling.
my $samps_ok = 1;
open my $du, "-|", "du -k output/*" or die;
while (my $rec = <$du>) {
    chomp $rec;

    my ($size, $name) = split /\s+/, $rec, 2;
    if ($size > 2000) {
        say "# Output split too big:";
        say "#  $name is $size kB";
        $samps_ok = 0;
        break;
    }
}
close $du;

ok($samps_ok, "Sample sizes even.");

# Check sorting.
my $sort_ok = 1;
open my $dd, "-|", "cat output/*" or die;
my $prev = <$dd>;
chomp $prev;

while (my $curr = <$dd>) {
    chomp $curr;

    if ($prev gt $curr) {
        say "# $prev > $curr";
        $sort_ok = 0;
        break;
    }

    $prev = $curr;
}
close $dd;

ok($sort_ok, "Sorted in order");
