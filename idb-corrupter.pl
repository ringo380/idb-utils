#!/usr/bin/perl

# ----------------------------------------
# InnoDB Corrupter
# ----------------------------------------
#
# Used to corrupt pages in a table for demonstration/testing purposes
#

use strict 'vars';
use warnings;

use Data::Dumper qw(Dumper);
use Fcntl qw(:seek);
use Getopt::Long qw(:config gnu_getopt);
use Scalar::Util qw(looks_like_number);

use constant {
		PAGE_SIZE            => 16384,
		FIL_PAGE_DATA        => 38,
		FSEG_HEADER_SIZE     => 10,
		PAGE_HEADER_PRIV_END => 26,
};

our ( $fh, $filename, $hex, $buffer, $page_count, $file_size);

our ( 
	$opt_help,
	$opt_debug,
	$opt_quiet,
	$opt_verbose,
	$opt_records,
	$opt_head,
	$multiplier,
	$page,
	$set_offset,
	$file
	);

GetOptions(
    'h'   => \$opt_help,
    'p=i' => \$page,
    'f=s' => \$file,
    'b=i' => \$multiplier,
	'o=i' => \$set_offset,
	'k'   => \$opt_head,
	'r'	  => \$opt_records,
    'd'   => \$opt_debug,
    'q'   => \$opt_quiet,
    'v'	  => \$opt_verbose,
) or die("Could not get options.\n");

$multiplier //= 1;

if ($opt_debug) { 
	print "Debugging enabled..\n";
}

if ($opt_help) {
	&usage;
	exit 0;
}

if ($file and $file =~ /ibd$/ ) { 
	$filename = $file; 
} else {
	print STDERR "Error: Filename invalid or no filename specified.\n";
	exit 0;
}

$file_size = -s $filename or print STDERR "Warning: Could not retrieve file size.\n";
$page_count = $file_size / PAGE_SIZE;
if (!$page) {
	print "No page specified. Choosing a random page..\n";
	$page = int(rand($page_count));
	print "Page number $page selected.\n";
}

sub usage {
	
    print <<END_OF_USAGE
Usage:
    idb-corrupter [-f <file>] [-p <page #>] [-e] [-r] [-k] [-v] [-vv] [-q] [-h] 
    where
         [-f[ile]] <file>        	Path to InnoDB data file. (Default behavior)
         [-p[age]] <page #>		 	Specify the page to corrupt.
		 [-b[ytes]]					Sets the amount of bytes to corrupt.
         [-v[erbose]]            	Displays additional information.
         [-d[ebug]] 				Displays debug output.
		 [-k]						Corrupt the page's FIL header area.
         [-r[ecords]]				Corrupt the record area specifically.
		 [-o[ffset]]				Set the exact byte offset you'd like to corrupt.
         [-e]						Displays empty pages.
         [-h[elp]]               	Displays this Usage information.
END_OF_USAGE
}

# ----------------------------------
# Utility Subs
# 

sub tohex {
    my $tohex = sprintf( "0x%x", $_[0] );
}

sub hr {
	printf "--------------------\n";
}

sub verbose {
	my ($string) = @_;
	if ($opt_verbose) { printf $string; } 
}

sub dbg { # Debug
	my ($string) = @_;
	if ($opt_debug) { printf $string; }
}

sub nl { # newline
	printf "\n";
}

#
# END Utility Subs
# ----------------------------------


sub corrupt_page {
	
	my ($offset, $f, $m, $l) = @_;
	my $len;
	my $string = '';
	
	for (1 .. $m ) {
		$string .= sprintf "%02X\n", rand(0xff);
	}
	
	chomp($f);
	
	if ($l) { 
		$len = $l;
	} else {
		$len = length($string);
	}
	
	my $data = pack "H*", $string;
	
	if ($opt_debug) {
		print "Data provided to sub 'corrupt_page'\n";
		print "\$offset = $offset\n";
		print "\$m = $m\n";
		print "\$f = $f\n";
		print "\$len = $len\n";
		print "\$string = $string\n";
	}
	
	print "Writing $len bytes of data to $f, in page $page at offset $offset..\n";
	open MODF, "+<$f"
      or die "Can't open $f for writing: $!\n";
	binmode MODF;
	sysseek( MODF, $offset, SEEK_SET )
	  or die "Can't seek to $offset bytes in $f: $!\n";
    syswrite( MODF, $data, $len )
	  or die "Can't write $string to $f: $!\n";
	print "Data written: ";
	print unpack "H*", $data;
	nl;
	print "Completed.\n"; 
    close MODF;
}

my $byte_start = $page * PAGE_SIZE;
if ($opt_debug) {
	print "Byte Start: $byte_start\n";
	print "\$Filename: $filename\n";
	print "\$Multiplier: $multiplier\n";
}

if ($opt_head) {
	$byte_start += int(rand(38));
	corrupt_page($byte_start, $filename, $multiplier)
	  or die "Could not corrupt $filename: $!\n";
	exit 0;
}

if ($opt_records) {
	my $increment = 128;
	my $val = int(rand(16248)); # 16384 - 128 - 8
	$byte_start += $increment;
	$byte_start += int(rand(16248));
	corrupt_page($byte_start, $filename, $multiplier)
	  or die "Could not corrupt $filename: $!\n";
	 exit 0;
};

corrupt_page($byte_start, $filename, $multiplier)
  or die "Could not corrupt $filename: $!\n";



