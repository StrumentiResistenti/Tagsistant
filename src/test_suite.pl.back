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

my $driver = undef;

#
# get C MACRO TAGSISTANT_INODE_DELIMITER
#
my $tagsistant_id_delimiter = qx(grep TAGSISTANT_INODE_DELIMITER tagsistant.h | cut -f3 -d ' ');
chomp $tagsistant_id_delimiter;
$tagsistant_id_delimiter =~ s/"//g;

if (defined $ARGV[0]) {
	if ($ARGV[0] eq "--mysql") {
		$driver = "mysql";
		system("echo 'drop table objects; drop table tags; drop table tagging; drop table relations;' | mysql -u tagsistant_test_suite --password='tagsistant_test' tagsistant_test");
	} elsif (($ARGV[0] eq "--sqlite") || ($ARGV[0] eq "--sqlite3")) {
		$driver = "sqlite3";
	} else {
		die("Please specify --mysql or --sqlite3");
	}
} else {
	die("Please specify --mysql or --sqlite3");
}

my $FUSE_GROUP = "fuse";

print "Testing with $driver driver\n";

# mount command
my $BIN = "./tagsistant";
my $MPOINT = "/tmp/tagsistant_test_suite";
my $MP = $MPOINT;
my $REPOSITORY = "$ENV{HOME}/.tagsistant_test_suite";
my $MCMD = "$BIN -s -d -v --repository=$REPOSITORY ";
if ($driver eq "mysql") {
	$MCMD .= "--db=mysql:localhost:tagsistant_test_suite:tagsistant_test:tagsistant_test";
} elsif ($driver eq "sqlite3") {
	$MCMD .= "--db=sqlite3";
} else {
	die("No driver!");
}
$MCMD .= " $MPOINT 2>&1";

# umount command
my $FUSERMOUNT = `which fusermount` || die("No fusermount found!\n");
chomp $FUSERMOUNT;
my $UMCMD = "$FUSERMOUNT -u $MPOINT";

# other global vars
my $TID = undef;
my $tc = 0;
my $tc_ok = 0;
my $tc_error = 0;
my $output = undef;
my $error_stack = "";

# here we create a garbage file and use it as a test dummy
# the file gets copied and linked inside multiple directories
# and than read again using diff. this ensures proper operations
# on open(), read(), write(), symlink() and readlink()
system("dmesg | tail > /tmp/clutter");
system("dd if=/dev/random of=/tmp/clutter2 bs=10 count=1");
system("dd if=/dev/random of=/tmp/clutter3 bs=10 count=1");
system("dd if=/dev/random of=/tmp/clutter4 bs=10 count=1");
system("dd if=/dev/random of=/tmp/clutter5 bs=10 count=1");
system("dd if=/dev/random of=/tmp/clutter6 bs=10 count=1");
system("dd if=/dev/random of=/tmp/clutter7 bs=10 count=1");
system("dd if=/dev/random of=/tmp/clutter8 bs=10 count=1");
system("dd if=/dev/random of=/tmp/clutter9 bs=10 count=1");

start_tagsistant();

# ---------[tagsistant is mounted, do tests]---------------------------- <---

my $testbed_ok = !test("ls -a $MP");

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

# first we create some directories
test("mkdir $MP/store/t1");
test("touch $MP/store/t1");
test("mkdir $MP/tags/t2");
test("stat $MP/store/t2");
test("stat $MP/tags/t1");
test("mkdir $MP/store/t3");
test("mkdir $MP/store/toberenamed");
test("mkdir $MP/store/tobedeleted");
test("ls $MP/tags");
test("ls $MP/archive");
test("stat $MP/store/t1");
test("ls $MP/store/t1");

# then we check rename and remove of tag directories
test("mv $MP/store/toberenamed $MP/store/renamed");
test("rmdir $MP/store/tobedeleted");

test("ls $MP/store/");
test("ls $MP/store/t1");
test("ls $MP/store/t1/@");
test("ls $MP/store/t1/@@");

test("cp /tmp/clutter $MP/store/t1/@/");
test("cp /tmp/clutter $MP/store/t2/@/");
test("stat $MP/store/t1/@/clutter");
test("stat $MP/store/t2/@/clutter");
test("stat $MP/store/t1/t2/@/clutter");
test("ln -s /tmp/clutter $MP/store/t3/@");
test("readlink $MP/store/t3/@/clutter");

test("ls -la $MP/archive/");
test("ls -la $MP/store/t1/@");
test("ls -la $MP/store/t1/+/t2/@");
test("ls -la $MP/store/t1/t2/@");
test("diff $MP/store/t1/@/clutter $MP/store/t2/@/clutter");
test("diff $MP/store/t1/@/clutter $MP/store/t2/@/clutter");

# then we rename a file
test("mv $MP/store/t1/@/clutter $MP/store/t1/@/clutter_renamed");
test("ls -la $MP/store/t1/@/");
test("stat $MP/store/t1/@/clutter_renamed");

# then we rename a file out of a directory into another;
# that means, from a tagsistant point of view, untag the
# file with all the tag contained in original path and
# tag it with all the tags contained in the destination
# path
test("stat $MP/store/t2/@/clutter_renamed");
test("mv $MP/store/t2/@/clutter_renamed $MP/store/t1/@/clutter");
test("ls -la $MP/store/t2/@");
test("ls -la $MP/store/t1/@");
test("ls -la $MP/store/t3/@/");
test("ls -la $MP/store/t1/t3/@/");

# now we check the unlink() method. first we copy two
# files in a tag directory. then we delete the first
# from the directory and the second from the archive/.
test("cp /tmp/clutter2 $MP/store/t1/@/tobedeleted");
test("cp /tmp/clutter3 $MP/store/t2/@/tobedeleted_fromarchive");
test("rm $MP/store/t1/@/tobedeleted");
test("ls -l $MP/store/t1/@/tobedeleted", 2); # we specify 2 as exit status 'cause we don't expect to find what we are searching

# now we create a file in two directories and than
# we delete if from just one. we expect the file to
# be still available in the other. then we delete
# from the second and last one and we expect it to
# desappear from the archive/ as well.
test("cp /tmp/clutter4 $MP/store/t1/t2/@/multifile");
test("stat $MP/store/t1/@/multifile");
test("stat $MP/store/t2/@/multifile");
test("rm $MP/store/t2/@/multifile");
test("ls -l $MP/store/t1/@/multifile", 0); # 0! we DO expect the file to be here
test("diff /tmp/clutter4 $MP/store/t1/@/multifile");
test("diff /tmp/clutter4 `find $MP/archive/|grep multifile`");
test("rm $MP/store/t1/@/multifile");
test("ls -l $MP/store/t1/@/multifile", 2); # 2! we DON'T expect the file to be here

# truncate() test
test("cp /tmp/clutter5 $MP/store/t1/@/truncate1");
test("truncate -s 0 $MP/store/t1/@/truncate1");
test("stat $MP/store/t1/@/truncate1");
out_test('Size: 0');
test("cp /tmp/clutter6 $MP/store/t2/@/truncate2");
test("truncate -s 10 $MP/store/t2/@/truncate2");
test("stat $MP/store/t2/@/truncate2");
out_test('Size: 10');

test("mkdir $MP/store/time:/");
test("mkdir $MP/store/time:/year/");
test("mkdir $MP/store/time:/year/eq/2000");
test("cp /tmp/clutter7 $MP/store/time:/year/eq/2000/@@");
test("stat $MP/store/time:/year/eq/2000/@@/clutter7");

test("mkdir $MP/store/time:/year/eq/2010");
test("cp /tmp/clutter8 $MP/store/time:/year/eq/2010/@@");
test("stat $MP/store/time:/year/eq/2010/@@/clutter8");
test("ls -la $MP/store/time:/year/gt/2005/@@");
out_test('clutter8');

test("stat $MP/store/time:/year/gt/2005/@@/*___clutter8");
goto OUT;

# ---------[no more test to run]---------------------------------------- <---
OUT:

print "\n" x 80;
print "*" x 80;
print "\nTests done! $tc test run - $tc_ok test succeeded - $tc_error test failed, summary follows:\n";

print $error_stack;

print "\n press [ENTER] to umount tagsistant...";
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
	print "Mounting tagsistant: $MCMD...\n";
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
	print "Creating the testbed...\n";

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
	print "Testbed running!\n";
}

sub stop_tagsistant {
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
