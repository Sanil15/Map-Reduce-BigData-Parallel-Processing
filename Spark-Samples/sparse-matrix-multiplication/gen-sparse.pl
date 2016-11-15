#!/usr/bin/perl
use 5.16.0;
use warnings FATAL => 'all';

my $nn = shift || 100;
my %hash = ();

for (my $jj = 0; $jj < $nn; ++$jj) {
    my $aa = int(rand(10));
   
    for (my $bb = 0; $bb < $aa; ++$bb) {
        my $ii = int(rand($nn));
        my $vv = rand(100);
        my $kk = "$jj, $ii";
        if (not exists $hash{ $kk })
        {
            say "$jj, $ii, $vv";
            $hash { $kk } = "$jj, $ii"
        }
    }
}