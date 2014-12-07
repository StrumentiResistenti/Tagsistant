#!/usr/bin/perl

use strict;
use warnings;

my $current_svn_revision = "";
my $current_build_number = 0;
my $current_build_date = 0;
my $current_build_revision = 0;

if (open(IN, "buildnumber.h")) {
	$current_build_number = <IN>;
	chomp($current_build_number);
	$current_build_number =~ s/#define TAGSISTANT_BUILDNUMBER //;

	if ($current_build_number =~ /"(\d+)\.(\d{8})\.(\d+)"/) {
		$current_svn_revision = $1;
		$current_build_date = $2;
		$current_build_revision = $3;
	}
	close IN;
} else {
	exit 1;
}

my ($day, $month, $year) = (localtime)[3,4,5];
$month = sprintf("%.2d", $month + 1);
$year += 1900;
$day = sprintf("%.2d", $day);

my $new_build_date = "$year$month$day";
my $new_build_revision = sprintf("%.6d", $current_build_revision + 1);

if ($new_build_date != $current_build_date) {
	print " *** resetting progressive\n";
	$new_build_revision = "000";
}

my @new_svn_revision = grep {/^Revision: .*/} `svn info http://tx0\@svn.gna.org/svn/tagfs`;
my $new_svn_revision = $new_svn_revision[0];
$new_svn_revision =~ s/Revision: //;
$new_svn_revision =~ s/\n//;
$new_svn_revision += 1;

my $new_build_number = "$new_svn_revision.$new_build_date.$new_build_revision";

print " *** \n";
print " *** \n";
print " *** \n";
print " *** Updated build number from $current_build_number to \"$new_build_number\"\n";
print " *** \n";
print " *** \n";
print " *** \n";

if (open(OUT, ">buildnumber.h")) {
	print OUT "#define TAGSISTANT_BUILDNUMBER \"$new_build_number\"";
	close OUT;
	system("touch tagsistant.c");
} else {
	exit 1;
}

exit 0;
