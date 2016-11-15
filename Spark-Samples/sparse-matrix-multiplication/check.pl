#!/usr/bin/perl
use 5.16.0;
use warnings FATAL => 'all';

my $aa_name = shift or die;
my $bb_name = shift or die;

open my $aah, "<", $aa_name;
open my $bbh, "<", $bb_name;
while (1) {
    my $aa = <$aah>;
    my $bb = <$bbh>;
    last unless (defined($aa) && defined($bb));

    chomp $aa; chomp $bb;
    my ($aj, $ai, $av) = split /\s*,\s*/, $aa;
    my ($bj, $bi, $bv) = split /\s*,\s*/, $bb;

    if ($aj != $bj || $ai != $bi) {
        die "Cell index mismatch: ($aj, $ai) != ($bj, $bi)";
    }

    if (sprintf("%.03f", $av) ne sprintf("%.03f", $bv)) {
        die "Value mismatch: $av != $bv";
    }
}

close $bbh;
close $aah;

say "Test OK - Matrix values match";
