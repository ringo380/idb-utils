#!/usr/bin/perl

use bytes;

#use strict;
use warnings;

#use diagnostics;
use Data::Dumper qw(Dumper);

use Fcntl qw(:seek);
use Getopt::Long qw(:config gnu_getopt);
use Scalar::Util qw(looks_like_number);

use constant {
    SIZE_FIL_HEAD    => '38',
    SIZE_FIL_TRAILER => '8',
    SIZE_PAGE        => '16384',
};

my ( $fh, $filename, $hex, $buffer );

my $POS_PAGE_BODY   = SIZE_FIL_HEAD;
my $POS_FIL_TRAILER = SIZE_PAGE - SIZE_FIL_TRAILER;

# Define page data variables
my (
    $checksum,    $spaceid,     $pagenum,  $lsn, $ptype,
    $page_n_heap, $oldchecksum, $low32lsn, $destfile
);

my $opt_help   = '';
my $opt_chop   = '';
my $set_page   = '';
my $file       = '';
my $find_page  = '';
my $opt_ibdata = '';
my $opt_debug  = '';
my $opt_quiet  = '';

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

my $mycnf_path  = "/root/.my.cnf";
my $mydata_path = "/var/lib/mysql";

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

    #my ($dec) = @_;
    #my $out = pack "n", $_;
    #my $tohex = unpack "H*", $out;
    my $tohex = sprintf( "0x%x", $_[0] );
}

sub get_bytes {
    my ( $byte_pos, $byte_count, $int );
    ( $byte_pos, $byte_count ) = @_;
    sysseek( $fh, $byte_pos, SEEK_SET )
      or die "Couldn't seek to byte $byte_pos in $filename";
    sysread( $fh, $buffer, $byte_count )
      or die "Could not read to byte $byte_pos in $filename";

    #print "Printing buffer: ";
    #printf '[%vd]', $buffer;
    #print "\n";
    if    ( $byte_count == 8 ) { $int = unpack "I*", $buffer; }
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
sub fil_page_type { get_bytes( ( cur_pos(@_) + 24 ), 2 ); }

# FIL Trailer data
sub fil_trailer_checksum { get_bytes( ( cur_pos(@_) + 16376 ), 4 ); }
sub fil_trailer_low32_lsn { get_bytes( ( cur_pos(@_) + 16380 ), 4 ); }

# Page Header data
sub page_n_heap { get_bytes( ( cur_pos(@_) + SIZE_FIL_HEAD + 4, 2 ) ); }

# FSP Header data
sub fsp_space_id { get_bytes( SIZE_FIL_HEAD, 4 ); }
sub fsp_high_page { get_bytes( SIZE_FIL_HEAD + 8, 4); }
sub fsp_flags { get_bytes( SIZE_FILE_HEAD + 16, 4); }

# sys_fsp_hdr
sub sys_hdr_checksum { get_bytes( 0, 4); }

sub sys_fsp_hdr {
	
	# Retrieve header values from ibdata1
	my $idb_checksum	= &getbytes( 0,	4);
	my $idb_offset		= &getbytes( 4, 4);
	my $idb_prev_page	= &getbytes( 8, 4);
	my $idb_next_page	= &getbytes( 12, 4);
	my $idb_lastmod_lsn = &getbytes( 16, 8);
	my $idb_page_type	= &getbytes( 24, 2);
	my $idb_flush_lsn	= &getbytes( 26, 8);
	my $idb_space_id	= &getbytes( 34, 4);
	
}

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
    $destfile = "$filename.pages." . $this_page;
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

    my ( $nam, $desc, $use ) = &get_page_type($ptype);
    my $cur_pos = cur_pos( fil_head_offset($pagenum) );
	$checksum = fil_head_checksum ($pagenum);

    printf "Page: ";
	printf fil_head_offset($pagenum);
	printf "\n";
    print "--------------------\n";
    printf "HEADER\n";
    printf "Byte Start: $cur_pos (" . tohex $offset;
    printf ")\n";
    printf "Page Type: $ptype - $nam: $desc - $use";
	if ($ptype == 0) {
		
	}
    printf "\nPAGE_N_HEAP (Amount of records in page): " . page_n_heap($pagenum) . "\n";
    printf "Prev Page: ";
    if   ( $prevpage == 4294967295 or !$prevpage ) { printf "Not used.\n"; }
    else                                           { printf "$prevpage\n"; }
    printf "Next Page: ";
    if   ( $nextpage == 4294967295 or !$prevpage ) { printf "Not used.\n"; }
    else                                           { printf "$nextpage\n"; }
    printf "LSN: $lsn\n";
    printf "Space ID: $spaceid\n";
    printf "Checksum: $checksum\n";
    printf "\nTRAILER\n";
    printf "Old-style Checksum: $oldchecksum\n";
    printf "Low 32 bits of LSN: $low32lsn\n";
    printf "Byte End: "
      . ( cur_pos($pagenum) + $page_size ) . " ("
      . tohex( cur_pos($pagenum) + $page_size );
    printf ")\n";
    print "--------------------\n";
}

if ($find_page) {
    my $datadir =
`mysqld --verbose --help 2> /dev/null | grep "datadir\\s" | sed 's/.*\\s//'`;
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
	print "$filename information:\n";
	print "---------------------\n";
	print "Checksum (FSP_HDR - 0, 4): " . sys_hdr_checksum . "\n";
	exit;
}

if ($set_page) {
    $page_start = $page_size * $set_page;
    if ( $page_start < $file_size and looks_like_number $set_page) {
        print_page( get_page($set_page) );
    }
    else {
        print "Invalid page.\n";
    }
}
else {
	
    print "Pages containing data in $filename:\n";
    print "--------------------\n";

    for ( $i = 0 ; $i < $page_count ; $i++ ) {
        unless ( !fil_head_checksum($i) ) {
            if ($opt_chop) {
                writepage( cur_pos($i), $i );
            }
            print_page( get_page($i) );
        }
    }
}
