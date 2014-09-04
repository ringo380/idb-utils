#!/usr/bin/perl

use bytes;

use strict;
use warnings;

#use diagnostics;
use Data::Dumper qw(Dumper);

use Fcntl qw(:seek);
use Getopt::Long qw(:config gnu_getopt);
use Scalar::Util qw(looks_like_number);
use Math::Int64 qw( :native_if_available int64 );

use constant {
    SIZE_FIL_HEAD    		=> '38',		# Page Header Size - Default 38
    SIZE_FIL_TRAILER 		=> '8',			# Page Trailer Size - Default: 8
    SIZE_PAGE        		=> '16384',		# Page Size - Default: 16384
    SIZE_LOG_BLOCK	 		=> '512',		# Log Block Size - Default: 512
	LOG_CHECKPOINT_GROUPS 	=> '32'    		# Maximum number of log group checkpoints
};

my ( $fh, $filename, $hex, $buffer );

my $POS_PAGE_BODY   = SIZE_FIL_HEAD;
my $POS_FIL_TRAILER = SIZE_PAGE - SIZE_FIL_TRAILER;

# Define page data variables
#my (
#    $checksum,    $spaceid,     $pagenum,  $lsn, $ptype,
#    $page_n_heap, $oldchecksum, $low32lsn, $destfile
#);

our ( 
	$opt_help,
	$opt_chop,
	$opt_head,
	$opt_ibdata,
	$opt_debug,
	$opt_quiet,
	$set_page,
	$file,
	$find_page
	);
	
#my $opt_help   = '';
#my $opt_chop   = '';
#my $opt_head   = '';
#my $set_page   = '';
#my $file       = '';
#my $find_page  = '';
#my $opt_ibdata = '';
#my $opt_debug  = '';
#my $opt_quiet  = '';

GetOptions(
    'h'   => \$opt_help,
    'k'   => \$opt_head,
    'c'   => \$opt_chop,
    'p=i' => \$set_page,
    'f=s' => \$file,
    's=i' => \$find_page,    # change to search
    'd'   => \$opt_debug,
    'q'   => \$opt_quiet,
    'i'   => \$opt_ibdata
) or die("Could not get options.\n");

if ($opt_debug) { 
	print "Debugging enabled..\n";
}

#------------------------------------------------------------------------------
# sub usage():
#       Displays usage information.
#
sub usage {
	
    print <<END_OF_USAGE
Usage:
    idb-parse [-f[ile] <file>] [-p[age] <page #>] [-i[bdata]]
            [-v[erbose]] [-d[ebug]] [-h[elp]]
    where
         [-f[ile]] <file>        Path to InnoDB data file. (Default behavior)
         [-p[age]] <page #>		 Specify a single page to get information from.
         [-i[bdata]] <file>		 Specify an InnoDB system data file (eg. ibdata1) to get information from.
         [-v[erbose]]            Displays additional information.
         [-d[ebug]]      		 Displays debug information.
         [-h[elp]]               Displays this Usage information.
END_OF_USAGE
}

#------------------------------------------------------------------------------
# sub about():
#       displays about information.
#
sub about {
    print <<END_OF_ABOUT
idb-parse

Ryan Robson,
ringo380\@gmail.com

\$Revision: 1.0 \$
\$Date: 2014/09/03 \$
END_OF_ABOUT
}


#------------------------------------------------------------------------------
# sub document():
#       displays full documentation.
#
sub document {
print <<'END_OF_DOCUMENTATION'
PERL                                                idb-parse(1)

NAME
    idb-parse - Parses InnoDB data from the file system.

SYNOPSIS
    idb-parse [-f[ile <file>] [-p[age]] [-i[bdata]
            [-v[erbose]] [-h[elp]] [-d[ebug]]

DESCRIPTION
    This program reads a G2++ file and converts it into XML.


ARGUMENTS
   -f[file] <file>
     Allows to specify a G2++ file to convert.

   [-v[erbose]]
     Displays ongoing information and details.

   [-H[ELP]]
     Displays the usage.
     If used with other options, causes immediate program termination.

   [-D[OCUMENT]]
     Displays the full documentation text (this file).
     If used with other options, causes immediate program termination.

   [-A[BOUT]]
     Displays informations about this program. Version, author, etc.
     If used with other options, causes immediate program termination.

EXAMPLES
    Convert the file ..\test_files\Test_01.g2++ into XML. Display the
    result on stdout.
       idb-parse -f ..\test_files\Test_01.g2++
    
    The output file will contain a comment specifying the original
    file and the translation date.
    
    The DTD of the output file will refer to a local DTD whose name is
    composed of the FA fields 'system' and 'trans'.

END_OF_DOCUMENTATION
}


#------------------------------------------------------------------------------
# sub relnotes():
#       displays release notes.
#
sub rel_notes {
    print <<END_OF_RELNOTES;
idb-parse
\$Revision: 1.0 \$
\$Date: 2002/03/21 14:17 \$
	Support unlimited G2++ structure, including arrays.

END_OF_RELNOTES
}

if ($opt_help) {
	usage;
	exit;
}

our $mycnf_path  = "/root/.my.cnf";
our $mydata_path = "/var/lib/mysql";

# Set the filename
if   ($file) { $filename = $file; }
else         { $filename = $ARGV[-1]; }
if ( !-e $filename ) {
    print STDERR "File path provided does not exist.\n";
    exit;
}
if ( !-r $filename ) {
    print STDERR "File path provided is not readable.\n";
    exit;
}

my $offset     = 0;
my $i          = 0;
my $page_size  = SIZE_PAGE;
my $file_size  = -s $filename;
my $page_count = $file_size / $page_size;

if ($opt_help) { print "This is help\n"; exit; }
if ($opt_chop) {
    print "Splitting into page files..\n";
    my $opt_chop = 1;
}

# ----- Define log record types

my %record_types = (
    1  => 'MLOG_1BYTE',
    2  => 'MLOG_2BYTE',
    4  => 'MLOG_4BYTE',
    8  => 'MLOG_8BYTE',
    9  => 'REC_INSERT',
    10 => 'REC_CLUST_DELETE_MARK',
    11 => 'REC_SEC_DELETE_MARK',
    13 => 'REC_UPDATE_IN_PLACE',
    14 => 'REC_DELETE',
    15 => 'LIST_END_DELETE',
    16 => 'LIST_START_DELETE',
    17 => 'LIST_END_COPY_CREATED',
    18 => 'PAGE_REORGANIZE',
    19 => 'PAGE_CREATE',
    20 => 'UNDO_INSERT',
    21 => 'UNDO_ERASE_END',
    22 => 'UNDO_INIT',
    23 => 'UNDO_HDR_DISCARD',
    24 => 'UNDO_HDR_REUSE',
    25 => 'UNDO_HDR_CREATE',
    26 => 'REC_MIN_MARK',
    27 => 'IBUF_BITMAP_INIT',
    28 => 'LSN',
    29 => 'INIT_FILE_PAGE',
    30 => 'WRITE_STRING',
    31 => 'MULTI_REC_END',
    32 => 'DUMMY_RECORD',
    33 => 'FILE_CREATE',
    34 => 'FILE_RENAME',
    35 => 'FILE_DELETE',
    36 => 'COMP_REC_MIN_MARK',
    37 => 'COMP_PAGE_CREATE',
    38 => 'COMP_REC_INSERT',
    39 => 'COMP_REC_CLUST_DELETE_MARK',
    40 => 'COMP_REC_SEC_DELETE_MARK',
    41 => 'COMP_REC_UPDATE_IN_PLACE',
    42 => 'COMP_REC_DELETE',
    43 => 'COMP_LIST_END_DELETE',
    44 => 'COMP_LIST_START_DELETE',
    45 => 'COMP_LIST_END_COPY_CREATE',
    46 => 'COMP_PAGE_REORGANIZE',
    47 => 'FILE_CREATE2',
    48 => 'ZIP_WRITE_NODE_PTR',
    49 => 'ZIP_WRITE_BLOB_PTR',
    50 => 'ZIP_WRITE_HEADER',
    51 => 'ZIP_PAGE_COMPRESS',
);

# ----- Define Undo Log Segments

my @undo_types = { 1 => 'UNDO_INSERT', 2 => 'UNDO_UPDATE' };

# ----- Define Page Types and corresponding values

my %page_types = (
    'ALLOCATED' => {
        'value'       => 0,
        'description' => 'Freshly allocated.',
        'usage'       => 'Page type field not initialized.',
    },
    'UNDO_LOG' => {
        'value'       => 2,
        'description' => "Undo log",
        'usage'       => "stores previous values of modified records",
    },
    'INODE' => {
        'value'       => 3,
        'description' => "File segment inode",
        'usage'       => "bookkeeping for file segments",
    },
    'IBUF_FREE_LIST' => {
        'value'       => 4,
        'description' => "Insert buffer free list",
        'usage'       => "bookkeeping for insert buffer free space management",
    },
    'IBUF_BITMAP' => {
        'value'       => 5,
        'description' => "Insert buffer bitmap",
        'usage'       => "bookkeeping for insert buffer writes to be merged",
    },
    'SYS' => {
        'value'       => 6,
        'description' => "System internal",
        'usage'       => "used for various purposes in the system tablespace",
    },
    'TRX_SYS' => {
        'value'       => 7,
        'description' => "Transaction system header",
        'usage' =>
          "bookkeeping for the transaction system in system tablespace",
    },
    'FSP_HDR' => {
        'value'       => 8,
        'description' => "File space header",
        'usage'       => "header page (page 0) for each tablespace file",
    },
    'XDES' => {
        'value'       => 9,
        'description' => "Extent descriptor",
        'usage'       => "header page for subsequent blocks of 16,384 pages",
    },
    'BLOB' => {
        'value'       => 10,
        'description' => "Uncompressed BLOB",
        'usage'       => "externally-stored uncompressed BLOB column data",
    },
    'ZBLOB' => {
        'value'       => 11,
        'description' => "First compressed BLOB",
        'usage' => "externally-stored compressed BLOB column data, first page",
    },
    'ZBLOB2' => {
        'value'       => 12,
        'description' => "Subsequent compressed BLOB",
        'usage' =>
          "externally-stored compressed BLOB column data, subsequent page",
    },
    'INDEX' => {
        'value'       => 17855,
        'description' => "B+Tree index",
        'usage'       => "table and index data stored in B+Tree structure",
    }
);

sub tohex {
    my $tohex = sprintf( "0x%x", $_[0] );
}

sub get_bytes {
    my ( $byte_pos, $byte_count, $int );
    ( $byte_pos, $byte_count ) = @_;
    sysseek( $fh, $byte_pos, SEEK_SET )
      or die "Couldn't seek to byte $byte_pos in $filename";
    sysread( $fh, $buffer, $byte_count )
      or die "Could not read to byte $byte_pos in $filename";
      
	if ($opt_debug) {
		print "Printing buffer: ";
		printf '[%vd]', $buffer;
		print "\n";
	}
	
    if    ( $byte_count == 8 ) { $int = hex unpack "H*", $buffer; }
    elsif ( $byte_count == 4 ) { $int = unpack "N*", $buffer; }
    elsif ( $byte_count == 2 ) { $int = unpack "n*", $buffer; }
    else                       { $int = unpack "C*", $buffer; }
    return $int;

}

# Set byte offset / current position
sub cur_pos {
    my ($page) = @_;
    $page_size * $page;
}

# FIL Header data
sub fil_head_offset { get_bytes( ( cur_pos(@_) + 4 ), 4 ); }
sub fil_head_checksum { get_bytes( cur_pos(@_), 4 ); }
sub fil_head_prev { get_bytes( ( cur_pos(@_) + 8 ), 4 ); }
sub fil_head_next { get_bytes( ( cur_pos(@_) + 12 ), 4 ); }
sub fil_head_space_id { get_bytes( ( cur_pos(@_) + 34 ), 4 ); }
sub fil_head_lsn { get_bytes( ( cur_pos(@_) + 20 ), 4 ); }
sub fil_head_page_type { get_bytes( ( cur_pos(@_) + 24 ), 2 ); }

# FIL Trailer data
sub fil_trailer_checksum { get_bytes( ( cur_pos(@_) + 16376 ), 4 ); }
sub fil_trailer_low32_lsn { get_bytes( ( cur_pos(@_) + 16380 ), 4 ); }

# Page Header data
sub page_n_heap { get_bytes( ( cur_pos(@_) + SIZE_FIL_HEAD + 4, 2 ) ); }

# FSP Header data
sub fsp_space_id { get_bytes( SIZE_FIL_HEAD, 4 ); }
sub fsp_high_page { get_bytes( SIZE_FIL_HEAD + 8, 4); }
sub fsp_flags { get_bytes( SIZE_FIL_HEAD + 16, 4); }

# ibdata1 header info - sys_fil_hdr
sub sys_hdr_checksum 	{ get_bytes( 0, 4 ); }
sub sys_hdr_offset 		{ get_bytes( 4, 4 ); }
sub sys_hdr_prev 		{ get_bytes( 8, 4 ); }
sub sys_hdr_next		{ get_bytes( 12, 4 ); }
sub sys_hdr_chkpnt_lsn  { get_bytes( 20, 4 ); }
sub sys_hdr_lastmod_lsn { get_bytes( 16, 8 ); }
sub sys_hdr_page_type	{ get_bytes( 24, 2 ); }
sub sys_hdr_flush_lsn	{ get_bytes( 26, 8 ); }
sub sys_space_id		{ get_bytes( 34, 4 ); }

# ib_logfile info
sub log_flush_lsn		{ get_bytes( ( SIZE_LOG_BLOCK + 12 ), 4 ); }



sub get_page {

    my $page = $_[0] or 0;
    my @attr;

    $offset = $page_size * $page;

    # Get FIL Header
    push @attr, get_bytes( ( $offset + 4 ), 4 );    # 0 page number
    push @attr, get_bytes( $offset, 4 );            # 1 checksum
    push @attr, get_bytes( ( $offset + 8 ),  4 );   # 2 prev page
    push @attr, get_bytes( ( $offset + 12 ), 4 );   # 3 next page
    push @attr, get_bytes( ( $offset + 34 ), 4 );   # 4 space id
    push @attr, get_bytes( ( $offset + 20 ), 4 );   # 5 lsn
    push @attr, get_bytes( ( $offset + 24 ), 2 );   # 6 page type

    # Get Page Header
    push @attr,
      get_bytes( ( $offset + 38 + 4 ), 2 )
      ;    # 7 PAGE_N_HEAP - amount of records in page

    # Get Trailer
    push @attr, get_bytes( ( $offset + 16376 ), 4 );    # 8 old-style checksum
    push @attr, get_bytes( ( $offset + 16380 ), 4 );    # 9 low 32 bits of lsn

    push @attr, $offset;    # 10 add the offset in there

    return @attr;
}

sub writepage {
    my ( $byte_pos, $this_page ) = @_;
    my $destfile = "$filename.pages." . $this_page;
    open OUTF, ">$destfile"
      or die "Can't open $destfile for writing: $!\n";
    binmode OUTF;
    sysseek( $fh, $byte_pos, SEEK_SET );
    sysread( $fh, $buffer, 16384 );
    syswrite( OUTF, $buffer );
    close OUTF;
}

sub get_page_type {
    my ($p) = @_;
    foreach my $k1 ( keys %page_types ) {
        if ( $page_types{$k1}{'value'} == $p ) {
            return (
                $k1,
                $page_types{$k1}{'description'},
                $page_types{$k1}{'usage'}
            );
        }
        else {
            next;
        }
    }
}



sub print_page {

    my (
        $pagenum,        # 0
        $checksum,       # 1
        $prevpage,       # 2
        $nextpage,       # 3
        $spaceid,        # 4
        $lsn,            # 5
        $ptype,          # 6
        $page_n_heap,    # 7
        $oldchecksum,    # 8
        $low32lsn,       # 9
        $offset          # 10
    ) = @_;



    printf "\nTRAILER\n";
    printf "Old-style Checksum: $oldchecksum\n";
    printf "Low 32 bits of LSN: $low32lsn\n";
    printf "Byte End: "
      . ( cur_pos($pagenum) + $page_size ) . " ("
      . tohex( cur_pos($pagenum) + $page_size );
    printf ")\n";
    print "--------------------\n";
}

sub print_fil_hdr {
	my ($p) = @_; 	# Get page number
	
	my $cur_pos 	= cur_pos( fil_head_offset($p) );
    my $prev		= fil_head_prev($p);
    my $next		= fil_head_next($p);
    my $offset		= fil_head_offset($p);
    my $type		= fil_head_page_type($p);
    my $pheap		= page_n_heap($p);
    my $lsn			= fil_head_lsn($p);
    my $id			= fil_head_space_id($p);
    my $checksum	= fil_head_checksum($p);
    
	
    my ( $nam, $desc, $use ) = get_page_type(fil_head_page_type($p));
       
	#my $checksum = fil_head_checksum($p);

    printf "Page: $offset\n";
    printf "--------------------\n";
    printf "HEADER\n";
    printf "Byte Start: $cur_pos (" . tohex $cur_pos;
    printf ")\n";
    printf "Page Type: $type - $nam: \n$desc - $use\n";
    printf "\nPAGE_N_HEAP (Amount of records in page): $pheap\n";
    printf "Prev Page: ";
    if   ( $prev == 4294967295 or !$prev ) { printf "Not used.\n"; }
    else                                           { printf "\n"; }
    printf "Next Page: ";
    if   ( $next == 4294967295 or !$next ) { printf "Not used.\n"; }
    else                                           { printf "$next\n"; }
    printf "LSN: $lsn\n";
    printf "Space ID: $id\n";
    printf "Checksum: $checksum\n";
}

sub print_fsp_hdr {
    printf "--------------------\n";
	printf "FSP_HDR - Filespace Header\n";
    printf "--------------------\n";
    printf "Space ID: " . fsp_space_id . "\n";
    printf "High Page: " . fsp_high_page . "\n";
}

if ($find_page) {
    my $datadir = `mysqld --verbose --help 2> /dev/null | grep \"datadir\\s\" | sed 's/.*\\s//'`;
    print "Datadir: $datadir\n";
    chomp($datadir);
    $datadir =~ s/\/$//;

#opendir(DH, $datadir) or die "Couldn't open directory handle on $datadir: $!\n";
#my @files = readdir(DH);
#closedir(DH);
    my @tblattr;
    my @files = <$datadir/*/*.ibd>;
    foreach my $tblfile (@files) {
        next unless $tblfile =~ /\.ibd$/;
        print "Checking $tblfile.. \n";
        $page_size  = 16384;
        $file_size  = -s $tblfile;
        $page_count = $file_size / $page_size;
        open( $fh, "<", $tblfile ) or die "Can't open $filename: $!";
        binmode($fh) or die "Can't binmode $filename: $!";
        for ( $i = 0 ; $i < $page_count ; $i++ ) {
            @tblattr = get_page($i);
            if ( $tblattr[0] == $find_page ) {
                print "Found page $find_page in $tblfile.\n";
                exit;
            }
        }
        close($fh);
    }
    exit;
}

$buffer = '';    # Clear out buffer

# Open up the tablespace file for binary reading
open( $fh, "<", $filename ) or die "Can't open $filename: $!";
binmode($fh) or die "Can't binmode $filename: $!";

if ($opt_ibdata) {
	print "Data File (ibdata) Information:\n";
	print "---------------------------\n";
	print "FSP_HDR - Filespace Header\n";
	print "---------------------------\n";
	print "Checksum (0, 4): " . sys_hdr_checksum . "\n";
	print "Last Modification LSN (16, 8): " . sys_hdr_lastmod_lsn . "\n";
	print "Flush LSN (26, 8): " . sys_hdr_flush_lsn . "\n";
	exit;
}

sub process_page {
		my $page_start = $page_size * $set_page;
		if ( $page_start < $file_size and looks_like_number $set_page) {
			print_page( get_page($set_page) );
		}
		else {
			print "Invalid page.\n";
		}
}

sub process_pages {
		print "Pages containing data in $filename:\n";
		print "--------------------\n";
		for ( $i = 0 ; $i < $page_count ; $i++ ) {
			unless ( !fil_head_checksum($i) ) {
				if ($opt_chop) {
					writepage( cur_pos($i), $i );
				}			
				#print_page( get_page($i) );
				print_fil_hdr($i);
				print_fsp_hdr;
			}
		}
}

if ($set_page) {
	process_page;
} else {
	process_pages;
}
