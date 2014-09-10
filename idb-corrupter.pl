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
		PAGE_SIZE	=> 	16384
};

our ( $fh, $filename, $hex, $buffer, $page_count, $file_size);

our ( 
	$opt_help,
	$opt_debug,
	$opt_quiet,
	$opt_verbose,
	$multiplier,
	$page,
	$file
	);

GetOptions(
    'h'   => \$opt_help,
    'p=i' => \$page,
    'f=s' => \$file,
    'm=i' => \$multiplier,
    'd'   => \$opt_debug,
    'q'   => \$opt_quiet,
    'v'	  => \$opt_verbose,
) or die("Could not get options.\n");

$multiplier //= 1;

if ($opt_debug) { 
	print "Debugging enabled..\n";
}

if ($file and $file =~ /ibd$/ ) { 
	$filename = $file; 
} else {
	print STDERR "Error: Filename invalid or no filename specified.\n";
	exit 0;
}

sub corrupt_page {
	
	my ($offset, $f, $m) = @_;
	my $string = '';
	
	for (1 .. $m ) {
		$string .= sprintf "%08X\n", rand(0xffffffff);
	}
	
	chomp($f);
	my $len = length($string);
	my $data = pack "H*", $string;
	
	if ($opt_debug) {
		print "Data provided to sub 'corrupt_page'\n";
		print "\$offset = $offset\n";
		print "\$m = $m\n";
		print "\$f = $f\n";
		print "\$len = $len\n";
		print "\$string = $string\n";
	}
	
	print "Writing $len bytes of data to $f at offset $offset..\n";
	open MODF, "+<$f"
      or die "Can't open $f for writing: $!\n";
	binmode MODF;
	sysseek( MODF, $offset, SEEK_SET )
	  or die "Can't seek to $offset bytes in $f: $!\n";
    syswrite( MODF, $data, $len )
	  or die "Can't write $string to $f: $!\n";
	print "Completed.\n"; 
    close MODF;
}

my $byte_start = $page * PAGE_SIZE;
if ($opt_debug) {
	print "Byte Start: $byte_start\n";
	print "\$Filename: $filename\n";
	print "\$Multiplier: $multiplier\n";
}
corrupt_page($byte_start, $filename, $multiplier)
  or die "Could not corrupt $filename: $!\n";



