#!/usr/bin/perl
#
# Find Tablespace ID

use strict;
use warnings;

use bytes;
use Fcntl qw(:seek);
use Getopt::Long qw(:config gnu_getopt);
#use Data::Dumper;

use constant {
    SIZE_FIL_HEAD    		=> 38,		# Page Header Size - Default 38
    SIZE_FIL_TRAILER 		=> 8,			# Page Trailer Size - Default: 8
    SIZE_PAGE        		=> 16384,		# Page Size - Default: 16384
};

our ( 	
		$fh, 
		$filename,
		$tablespaces
	);
	
our (						# GetOpt Switches/Arguments
		$opt_help,
		$opt_list,
		$tsid,
		$datadir,
		$set_db			
	);
	
our $mydata_path = "/var/lib/mysql";
	
GetOptions(
    'h'   	=> \$opt_help,
    'l'		=> \$opt_list,
    'd=s' 	=> \$datadir,
    's=s'   => \$set_db,
    't=i' 	=> \$tsid
) or die("Could not get options.\n");

########################################################
# USAGE
#
my $USAGE =<<USAGE;

     Usage:

         idb-findtsid [-h[elp]] [-l[ist]] [-d[atadir] <path>] [-t[ablespace id] <#>]
						[-s[et database] <database>]

         where:

             -h  This usage information
             -l  List tablespace IDs of all table files in the data directory
             -s  List tablespace IDs in a particular database
             -d  Set the MySQL data directory
             -t  Find table file from a provided tablespace ID number

USAGE
#
######################################################

if ($opt_help) { print "$USAGE\n"; exit 0; }		# Display usage if -h is given

unless ($datadir) { 
	$datadir = `mysqld --verbose --help 2> /dev/null | grep "datadir\\s" | sed 's/datadir[ ]*//'`
		or die "Could not determine data directory. Please specify a data directory manually with the '-d' flag.\n";
	$datadir =~ s/\/$//;
	chomp($datadir);
}

sub get_bytes {
	
    my ( $byte_pos, $byte_count ) = @_;
    
    my $hex;
    my $buffer = '';
    
    sysseek( $fh, $byte_pos, SEEK_SET ) or die "Couldn't seek to byte $byte_pos in $filename";
    sysread( $fh, $buffer, $byte_count ) or die "Could not read to byte $byte_pos in $filename";
    $hex = unpack "H*", $buffer;
    return hex $hex;
}

sub fsp_space_id { get_bytes( SIZE_FIL_HEAD, 4 ); }

sub get_tsids {
	
	my $sid;
	my $results;
	
	if ($_[0]) { ($sid) = @_; }
	
	my @files = <$datadir/*/*.ibd>;
	
	foreach my $tblfile (@files) {
		next unless $tblfile =~ /\.ibd$/;
		open( $fh, "<", $tblfile ) or die "Can't open $tblfile: $!";
		binmode($fh) or die "Can't binmode $tblfile: $!";
		my $tsid = fsp_space_id();
		if ($sid) {
			next unless ($tsid == $sid);
			$results->{ $tblfile } = $tsid;
		} else {
			$results->{ "$tblfile" } = $tsid;
		}
	}
	return $results;
}

if ($opt_list) { 
	$tablespaces = get_tsids();
	while( my ($k, $v) = each %$tablespaces ) {
		$k =~ s/^$datadir\///;
        print "$k - Space ID: $v\n";
    }
    exit 0;
}

if ($tsid) {
	$tablespaces = get_tsids($tsid);
	while( my ($k, $v) = each %$tablespaces ) {
		$k =~ s/^$datadir\///;
        print "$k - Space ID: $v\n";
    }
    exit 0;
}





#printf "$tblfile - Space ID: $tsid\n";	





