#!/usr/bin/perl

use strict;
use warnings;

unless ($ARGV[0]) {
	print qq|NO file passed!\n|;
	die();
}

my %lines = ();

open IN, $ARGV[0];
while (<IN>) {
	if (/^(0x[0-9a-f]{8,8}): (.*)/i) {
		next if $2 =~ /free\(\)/;
		$lines{$1} = $2;
	}
}
close IN;

open IN, $ARGV[0];
while (<IN>) {
	if (/^(0x[0-9a-f]{8,8}): free\(\)/i) {
		delete($lines{$1}) if defined $lines{$1};
	}
}
close IN;

for my $key (sort keys %lines) {
	print qq|$key: $lines{$key}\n|;
}
