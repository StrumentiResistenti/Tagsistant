#!/usr/bin/perl

#
# test script for tagsistant
#
# using perl threads, the script starts a tagsistant instance
# on a thread, fetching all the output and feeding it to a
# file. on the main thread the script performs a sequence of
# tests and reports any error on STDERR
#

use strict;
use warnings;

use threads;
use threads::shared;

use Errno;
use POSIX;

our ($FUSE_GROUP, $MP, $REPOSITORY, $MCMD, $UMCMD, $TID, $tc, $tc_ok, $tc_error, $error_stack, $output);

start();

# ---------[tagsistant is mounted, do tests]---------------------------- <---

#
# Standard tagsistant directories
#
test("ls -a $MP/archive");
out_test('^\.$', '^\.\.$');
test("ls -a $MP/stats");
out_test('^\.$', '^\.\.$');
test("ls -a $MP/relations");
out_test('^\.$', '^\.\.$');
test("ls -a $MP/alias");
out_test('^\.$', '^\.\.$');
test("ls -a $MP/store");
out_test('^\.$', '^\.\.$');

#
# Create three tags called tag1, tag2 and tag3 used in later tests
#
test("mkdir $MP/store/tag1");
test("touch $MP/store/tag1");
test("mkdir $MP/tags/tag2");
test("stat $MP/store/tag2");
test("stat $MP/tags/tag1");
test("mkdir $MP/store/tag3");
test("ls $MP/tags");
test("ls $MP/archive");
test("stat $MP/store/tag1");
test("ls $MP/store/tag1");

#
# rename and remove of tag directories
#
test("mkdir $MP/store/toberenamed");
test("mkdir $MP/store/tobedeleted");
test("mv $MP/store/toberenamed $MP/store/renamed");
test("rmdir $MP/store/tobedeleted");
test("stat $MP/store/renamed");
test("stat $MP/store/tobedeletd", 1);

#
# Basic query syntax
#
test("ls $MP/store/");
test("ls $MP/store/tag1");
test("ls $MP/store/tag1/@");
test("ls $MP/store/tag1/@@");
test("stat $MP/store/tag1/tag2/@@/error", 1);
test("stat $MP/store/tag1/+/tag2/@@/error", 1);
test("stat $MP/store/tag1/-/tag2/@@/error", 1);

#
# tagging the same file as tag1 and tag2
#
test("cp /tmp/file1 $MP/store/tag1/@/");
test("cp /tmp/file1 $MP/store/tag2/@/");
test("stat $MP/store/tag1/@/file1");
test("stat $MP/store/tag2/@/file1");
test("stat $MP/store/tag1/tag2/@/file1");

#
# tagging by link  a file as tag3
#
test("ln -s /tmp/file1 $MP/store/tag3/@");
test("readlink $MP/store/tag3/@/file1");

test("ls -la $MP/archive/");
test("ls -la $MP/store/tag1/@");
test("stat $MP/store/tag1/+/tag2/@/file1");
test("stat $MP/store/tag1/tag2/@/file1");
test("stat $MP/store/tag1/-/tag2/@/file1", 1);
test("diff $MP/store/tag1/@/file1 $MP/store/tag2/@/file1");

#
# object renaming
#
test("mv $MP/store/tag1/@/file1 $MP/store/tag1/@/file_renamed");
test("ls -la $MP/store/tag1/@/");
test("stat $MP/store/tag1/@/file_renamed");
test("stat $MP/store/tag2/@/file_renamed");

#
# then we rename a file out of a directory into another;
# that means, from a tagsistant point of view, untag the
# file with all the tag contained in original path and
# tag it with all the tags contained in the destination
# path
#
test("mv $MP/store/tag2/@/file_renamed $MP/store/tag1/@/file1");
test("stat $MP/store/tag2/@/file_renamed", 1);
test("stat $MP/store/tag1/@/file1");
test("stat /tmp/tagsistant_test_suite/store/tag1/+/tag3/@@/1___file1");
out_test('regular file');
test("stat /tmp/tagsistant_test_suite/store/tag1/+/tag3/@@/3___file1");
out_test('symbolic link');
test("stat $MP/store/tag1/tag3/@/file1", 1);

#
# object removal test: unlink()
#
test("cp /tmp/file2 $MP/store/tag1/@/tobedeleted");
test("rm $MP/store/tag1/@/tobedeleted");
test("ls -l $MP/store/tag1/@/tobedeleted", 2); # we specify 2 as exit status 'cause we don't expect to find what we are searching

#
# now we create a file in two directories and than
# we delete if from just one. we expect the file to
# be still available in the other. then we delete
# from the second and last one and we expect it to
# desappear from the archive/ as well.
#
test("cp /tmp/file4 $MP/store/tag1/tag2/@/multifile");
test("stat $MP/store/tag1/@/multifile");
test("stat $MP/store/tag2/@/multifile");
test("rm $MP/store/tag2/@/multifile");
test("stat $MP/store/tag1/@/multifile", 0); # 0! we DO expect the file to be here
test("diff /tmp/file4 $MP/store/tag1/@/multifile");
test("diff /tmp/file4 `find $MP/archive/|grep multifile`");
test("rm $MP/store/tag1/@/multifile");
test("stat $MP/store/tag1/@/multifile", 1); # 2! we DON'T expect the file to be here

#
# truncate() test. every file is at least 32 char long for the MD5
# hash, plus the dmesg lines (look at the setup_input_files subroutine
# of this test suite for more details)
#
test("cp /tmp/file5 $MP/store/tag1/@/truncate1");
test("truncate -s 0 $MP/store/tag1/@/truncate1");
test("stat $MP/store/tag1/@/truncate1");
out_test('Size: 0');
test("cp /tmp/file6 $MP/store/tag2/@/truncate2");
test("truncate -s 10 $MP/store/tag2/@/truncate2");
test("stat $MP/store/tag2/@/truncate2");
out_test('Size: 10');

#
# triple tags: the first is time:/year/eq/2000/
#
test("mkdir $MP/store/time:/");
test("mkdir $MP/store/time:/year/");
test("mkdir $MP/store/time:/year/eq/2000");
test("cp /tmp/file7 $MP/store/time:/year/eq/2000/@@");
test("stat $MP/store/time:/year/eq/2000/@@/file7");

#
# triple tags: the second is time:/year/eq/2010/
#
test("mkdir $MP/store/time:/year/eq/2010");
test("cp /tmp/file8 $MP/store/time:/year/eq/2010/@@");
test("stat $MP/store/time:/year/eq/2010/@@/file8");
test("ls -la $MP/store/time:/year/gt/2005/@@");
out_test('file8');

#
# triple tags: then we test disequality operators
#
test("stat $MP/store/time:/year/gt/2005/@@/*___file8");
test("ls $MP/store/time:/year/gt/1999/@@/*___file7");
test("stat $MP/store/time:/year/lt/3000/@/*___file8");

#
# relations: the includes/ and is_equivalent/ relations
#
test("mkdir $MP/relations/tag1/is_equivalent/tag2");
test("stat $MP/relations/tag1/is_equivalent/tag2");
test("mkdir $MP/relations/tag2/includes/tag3");
test("stat $MP/relations/tag2/includes/tag3");
test("stat $MP/store/tag1/@/truncate2");
test("stat $MP/store/tag1/@@/truncate2", 1);
test("ls $MP/store/tag1/@@/");
test("ls $MP/store/tag1/@/*___file1");
test("stat $MP/store/tag2/@/3___file1");
test("stat $MP/store/tag2/@@/3___file1", 1);
test("stat $MP/store/tag1/@/3___file1");
test("stat $MP/store/tag1/@@/3___file1", 0); # this should fail!

#
# relations: the excludes/ relation
#
test("mkdir $MP/store/tag4/");
test("cp /tmp/file9 $MP/store/tag1/tag4/@");
test("stat $MP/store/tag1/@/file9");
test("mkdir $MP/relations/tag1/excludes/tag4");
test("stat $MP/store/tag1/@/file9");

#
# directory objects
#
test("mkdir $MP/store/tag4/@@/dirobj/");
test("cp /tmp/file10 $MP/store/tag4/@@/dirobj/");
test("stat $MP/store/tag4/@@/dirobj/");
test("stat $MP/store/tag4/@@/dirobj/file10");

#
# the stats/ dir
#
test("stat $MP/stats/configuration");
test("cat $MP/stats/configuration");
out_test("mountpoint: $MP");

#
# the alias/ dir
#
test("echo 'tag1/+/tag2/' > $MP/alias/a1");
test("stat $MP/store/=a1/");
test("ls $MP/store/=a1/@/");

#
# the .tags suffix
#
test("stat $MP/store/tag1/@/truncate1.tags");
test("cat $MP/store/tag1/@/truncate1.tags");
test("cat $MP/store/tag1/@@/file9.tags");
out_test("tag1");
out_test("tag4");

# ---------[no more test to run]---------------------------------------- <---
OUT:

print "\n" x 10;
print "*" x 70, "\n";
print "* RESULTS: \n* \n";
print "* performed tests: $tc\n";
print "* succeded tests: $tc_ok\n";
print "* failed tests: $tc_error\n* \n";
print "* details on failed tests:\n" if $tc_error;

print $error_stack;

print "*" x 70, "\n\n";
print "press [ENTER] to umount tagsistant...";
<STDIN>;

EXITSUITE: stop_tagsistant();
$TID->join();

exit();

# ---------[script end, subroutines follow]-----------------------------

#
# the core of the thread running tagistant, invoked
# by start_tagsistant()
#
sub run_tagsistant {
	our ($MCMD);

	sleep(1);

	print "*" x 70, "\n";
	print "* Mounting tagsistant: $MCMD...\n";

	open(TS, "$MCMD|") or die("Can't start tagsistant ($MCMD)\n");
	open(LOG, ">/tmp/tagsistant.log") or die("Can't open log file /tmp/tagsistant.log\n");

	while (<TS>) {
		print LOG $_ if /^TS/;
	}

	close(LOG);
	threads->exit();
}

#
# prepare the testbad
#
sub start_tagsistant {
	our ($FUSE_GROUP, $REPOSITORY, $MCMD, $TID);

	#
	# check if user is part of fuse group
	#
	my $id = qx|id|;
	unless ($id =~ /\($FUSE_GROUP\)/) {
		die("User is not in $FUSE_GROUP group and is not allowed to mount a tagsistant filesystem");
	}

	#
	# remove old copy of the repository
	# each run must start from zero!
	#
	system("rm -rf $REPOSITORY 1>/dev/null 2>/dev/null");
	die("Can't remove $REPOSITORY\n") unless $? == 0;

	#
	# start tagsistant in a separate thread
	#
	$TID = threads->create(\&run_tagsistant);

	#
	# wait until tagsistant is brought to life
	#
	sleep(3);

	#
	# check if thread was properly started
	#
	unless (defined $TID and $TID) {
		die("Can't create tagsistant thread!\n");
	}
	print "*" x 70, "\n";
	print "* Testbed running!\n";
}

sub stop_tagsistant {
	our $UMCMD;
	print "\nUnmounting tagsistant: $UMCMD...\n";
	system($UMCMD);
}

#
# Execute a command and check its exit code
#
sub test {
	$tc++;

	#
	# build the command
	#
	#my $command = join(" ", @_);
	my $command = shift();

	#
	# expected exit status
	#
	my $expected_exit_status = shift() || 0;

	#
	# run the command and trap the output in a global variable
	#
	$output = qx|$command 2>&1|;

	#
	# guess the operation status (OK or ERROR!)
	#
	my $cmdexit = $? >> 8;
	my $status = ($cmdexit == $expected_exit_status) ? "[  OK  ]" : "[ERROR!] ($cmdexit)";

	#
	# print summary
	#
	my $status_line = "\n____ $status [#$tc] $command " . "_" x (60 - length($tc) - length($command)) . "cmd__\n\n$output";
	print $status_line;
	$error_stack .= $status_line if $status =~ /ERROR/;

	#
	# return 0 if everything went OK
	#
	if ($status =~ /OK/) {
		$tc_ok++;
		return 0;
	}

	#
	# report errors
	#
	my $signal = $? & 127;
	my $coredump = $? & 128;
	my $exitstatus = $? >> 8;
	my $strerror = strerror($exitstatus);

	if ($? == -1) {
		print " *** failed to execute: $!\n";
	} elsif ($signal) {
		printf " *** child died with signal %d, %s coredump\n", ($signal),  ($coredump) ? 'with' : 'without';
	} else {
		printf " *** child exited with value %d: %s\n", $exitstatus, $strerror;
	}

	$tc_error++;
	return $exitstatus;
}

#
# apply a list of regular expressions on the output
# of last performed command
#
sub out_test {
	my $stc = 0;
	my $status = undef;
	my $got_an_error = 0;
	$tc++;
	for my $rx (@_) {
		$stc++;
		unless ($output =~ m/$rx/m) {
			$got_an_error++;
			$status = "[ERROR!]";
		} else {
			$status = "[  OK  ]";
		}
		my $status_line = "\n \\__ $status [#$tc.$stc] /$rx/ " . "_" x (56 - length($tc) - length($stc) - length($rx)) . "__re__\n";
		print $status_line;
		$error_stack .= $status_line if $status =~ /ERROR/;
	}

	if ($got_an_error) {
		$tc_error++;
	} else {
		$tc_ok++;
	}
	return 0;
}

#
# here we create some garbage file to be used as test objects.
# The first one is just a dump of the last lines of dmesg.
# From the second file on, each file contains the MD5 sum of
# the previous plus the last lines of dmesg again.
# This ensures different files that can be distinguished by
# deduplication
#
sub setup_input_files {
	my $how_much = shift();

	my $_ = `dmesg`;

	for (my $i = 0; $i <= $how_much; $i++) {

		open(my $fh, ">", "/tmp/file$i");
		for (my $j = 0; $j < 10; $j++) {
			print $fh $_;
		}
		close $fh;
		
		tr/A-Za-z/D-ZA-Cd-za-c/;
	}
}

#
# prepare the testbed
#
sub start {
	my $driver = undef;

	print "*" x 70, "\n";
	print "* \n";
	print "* TAGSISTANT test suite\n";
	print "* \n";
	
	#
	# get C MACRO TAGSISTANT_INODE_DELIMITER
	#
	my $tagsistant_id_delimiter = qx(grep TAGSISTANT_INODE_DELIMITER tagsistant.h | cut -f3 -d ' ');
	chomp $tagsistant_id_delimiter;
	$tagsistant_id_delimiter =~ s/"//g;
	
	if (defined $ARGV[0]) {
		if ($ARGV[0] eq "--mysql") {
			$driver = "mysql";
			system("echo 'drop table objects; drop table tags; drop table tagging; drop table relations; drop table aliases;' | mysql -u tagsistant_test --password='tagsistant_test' tagsistant_test_suite");
		} elsif (($ARGV[0] eq "--sqlite") || ($ARGV[0] eq "--sqlite3")) {
			$driver = "sqlite3";
		} else {
			die("Please specify --mysql or --sqlite3");
		}
	} else {
		die("Please specify --mysql or --sqlite3");
	}
	
	our $FUSE_GROUP = "fuse";
	
	print "*" x 70, "\n";
	print "* Testing with $driver driver\n";
	
	# mount command
	my $BIN = "./tagsistant";
	my $MPOINT = "/tmp/tagsistant_test_suite";
	our $MP = $MPOINT;
	our $REPOSITORY = "$ENV{HOME}/.tagsistant_test_suite";
	$REPOSITORY = "/tmp/tagsistant_test_suite_repository";
	our $MCMD = "$BIN -s -d --debug=s -v --repository=$REPOSITORY ";
	if ($driver eq "mysql") {
		$MCMD .= "--db=mysql:localhost:tagsistant_test_suite:tagsistant_test:tagsistant_test";
	} elsif ($driver eq "sqlite3") {
		$MCMD .= "--db=sqlite3";
	} else {
		die("No driver!");
	}
	$MCMD .= " $MPOINT 2>&1";

	# print $MCMD, "\n";

	# umount command
	my $FUSERMOUNT = `which fusermount` || die("No fusermount found!\n");
	chomp $FUSERMOUNT;
	our $UMCMD = "$FUSERMOUNT -u $MPOINT";
	
	# other global vars
	our $TID = undef;
	our $tc = 0;
	our $tc_ok = 0;
	our $tc_error = 0;
	our $output = undef;
	our $error_stack = "";
	
	print "*" x 70, "\n";
	print "* Setting up input files... ";

	setup_input_files(20);

	print "done!\n";
	
	print "*" x 70, "\n";
	print "* Creating the testbed... done!\n";

	start_tagsistant();

	our $testbed_ok = !test("ls -a $MP");

	unless ($testbed_ok) {
		print "Testbed not ok!\n";
		goto EXITSUITE;
	}
	
	$testbed_ok = !test("ls -a $MP/tags");
	
	unless ($testbed_ok) {
		print "Testbed not ok!\n";
		goto EXITSUITE;
	}

	out_test('^\.$', '^\.\.$');

	print "*" x 70, "\n";
	print "* Press [ENTER] to start...";

	<STDIN>;
}
