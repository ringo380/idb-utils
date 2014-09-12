#!/usr/bin/perl


use strict 'vars';
use warnings;

use Data::Dumper qw(Dumper);
use Fcntl qw(:seek);
use Getopt::Long qw(:config gnu_getopt);
use Scalar::Util qw(looks_like_number);
use Math::Int64 qw( :native_if_available int64 hex_to_uint64 uint64_to_hex );

use constant {
    SIZE_FIL_HEAD    		=> 38,			# Page Header Size - Default 38
    SIZE_FIL_TRAILER 		=> 8,			# Page Trailer Size - Default: 8
    SIZE_PAGE        		=> 16384,		# Page Size - Default: 16384
    UT_HASH_RANDOM_MASK    	=> 1463735687,
	UT_HASH_RANDOM_MASK2    => 1653893711,
	FIL_PAGE_DATA			=> 38,			# Start of data on page
	FSEG_PAGE_DATA			=> 38, 			# Should be the same as FIL_PAGE_DATA - Start of file segment information n in
	IDX_HDR_SIZE			=> 36,
	FSEG_HEADER_SIZE		=> 10,			# Length of file system header, in bytes
	FIL_PAGE_LSN          	=> 16,
	FIL_PAGE_FILE_FLUSH_LSN => 26,
	PAGE_HEADER				=> 38, 			# Should be the same as FSEG_PAGE_DATA
	PAGE_HEADER_PRIV_END	=> 26, 			# end of private data structure of the page header which are set in a page create
	FIL_PAGE_OFFSET     	=> 4,
	FIL_PAGE_DATA       	=> 38,
	FLST_BASE_NODE_SIZE		=> 16,			# The physical size of a list base node in bytes
	REC_N_OLD_EXTRA_BYTES	=> 6,			# Number of extra bytes in an old-style record, in addition to the data and the offsets
	REC_N_NEW_EXTRA_BYTES	=> 5,			# Number of extra bytes in a new-style record, in addition to the data and the offsets
	FIL_PAGE_END_LSN_OLD_CHKSUM 	=> 8,
	FIL_PAGE_SPACE_OR_CHKSUM 		=> 0,
	FSP_HEADER_SIZE			=> 112,			# File space header size
	USR_REC_START			=> 120,
	SYS_REC_START			=> 94,
	# Directions of cursor movement:
	PAGE_LEFT				=> 1,
	PAGE_RIGHT				=> 2,
	PAGE_SAME_REC			=> 3,
	PAGE_SAME_PAGE			=> 4,
	PAGE_NO_DIRECTION		=> 5
};

our ( $fh, $filename, $hex, $buffer, $page_count, $file_size);

our $POS_PAGE_BODY   			= SIZE_FIL_HEAD;
our $POS_FIL_TRAILER 			= SIZE_PAGE - SIZE_FIL_TRAILER;
our $PAGE_SIZE	 				= SIZE_PAGE;

#------------------------------------------------------------------------------
# Most of the following grabbed from page0page.h in MySQL source to define 
# various page offset constants.
#

our $PAGE_DATA					= PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE; # Default = 94; defines start of page data
our $PAGE_OLD_INFIMUM			= $PAGE_DATA + 1 + REC_N_OLD_EXTRA_BYTES; # Default = 101; offset of the page infimum record on an old-style page
our $PAGE_OLD_SUPREMUM			= $PAGE_DATA + 2 + 2 * REC_N_OLD_EXTRA_BYTES + 8; # Default = 112; offset of the page supremum record on an old-style page
our $PAGE_OLD_SUPREMUM_END 		= $PAGE_OLD_SUPREMUM + 9; # Default = 121; offset of the page supremum record end on an old-style page
our $PAGE_NEW_INFIMUM			= $PAGE_DATA + REC_N_NEW_EXTRA_BYTES; # Default = 99; offset of the page infimum record on a new-style compact page
our $PAGE_NEW_SUPREMUM			= $PAGE_DATA + 2 * REC_N_NEW_EXTRA_BYTES + 8; # Default = 109; offset of the page supremum record on a new-style compact page
our $PAGE_NEW_SUPREMUM_END 		= $PAGE_NEW_SUPREMUM + 8; # Default = 117; offset of the page supremum record end on a new-style compact page

# Heap numbers
our $PAGE_HEAP_NO_INFIMUM	= 0; # page infimum
our $PAGE_HEAP_NO_SUPREMUM	= 1; # page supremum
our $PAGE_HEAP_NO_USER_LOW	= 2; # first user record in	creation (insertion) order, not necessarily collation order; this record may have been deleted
				
our $i = 0;				

# Record status values
our %rec_types = (
	0 	=> 'REC_STATUS_ORDINARY',
	1	=> 'REC_STATUS_NODE_PTR',
	2 	=> 'REC_STATUS_INFIMUM',
	3	=> 'REC_STATUS_SUPREMUM'
);
			

our %page_types = (
    'ALLOCATED' => {
        'value'       => 0,
        'description' => 'Freshly allocated',
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

our ( 
	$opt_help,
	$opt_head,
	$opt_chop,
	$opt_csum,
	$opt_ibdata,
	$opt_index,
	$opt_debug,			# -x
	$opt_quiet,
	$opt_verbose,
	$opt_vv,
	$opt_empty,
	$opt_records,
	$opt_list,
	$mode,
	$set_page,
	$set_type,
	$datadir,
	$file
	);

GetOptions(
    'h'   => \$opt_help,
    'k'   => \$opt_head,
    'p=i' => \$set_page,
    'f=s' => \$file,
    'c'   => \$opt_chop,	# Split into page file(s).
    's'	  => \$opt_csum,
	'l'   => \$opt_list,
    'x'   => \$opt_debug,
	't=s' => \$set_type,
	'm=s' => \$mode,
    'd'   => \$datadir,
    'q'   => \$opt_quiet,
    'b'   => \$opt_ibdata,
	'i'	  => \$opt_index,
    'v'	  => \$opt_verbose,
    'vv'  => \$opt_vv,
    'e'	  => \$opt_empty,	# Show empty pages
    'r'	  => \$opt_records
) or die("Could not get options.\n");

if ($opt_debug) { 
	print "Debugging enabled..\n";
}

unless ($datadir) {
	$datadir = `mysqld --verbose --help 2> /dev/null | grep "datadir\\s" | sed 's/datadir[ ]*//'`;
	$datadir =~ s/\/$//;
	chomp($datadir);
}
#------------------------------------------------------------------------------
# sub usage():
#       Displays usage information.
#
sub usage {
	
    print <<END_OF_USAGE
Usage:
    idb-pages [-f <file>] [-p <page #>] [-e] [-r] [-k] [-v] [-vv] [-q] [-h] 
    where
         [-f[ile]] <file>        	Path to InnoDB data file. (Default behavior)
         [-p[age]] <page #>		 	Specify a single page to get information from.
         [-v[erbose]]            	Displays additional information.
         [-d] <path>				Sets the path to the MySQL data directory.
         [-x]      		 			Displays debug information.
         [-r]						Displays page record information.
         [-k]						Displays only page header data.
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

sub tobin {
	printf "%b\n", $_;
}

sub bin2dec {
    return unpack("N", pack("B32", substr("0" x 32 . shift, -32)));
}

sub hr {
	printf "--------------------\n";
}

sub verbose {
	my ($string) = @_;
	if ($opt_verbose) { printf $string; } 
}

sub vv { # Very verbose
	my ($string) = @_;
	if ($opt_vv) { printf $string; }
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

# ----------------------------------
# Binary Operation Subs
#

sub get_bytes {
	
	open( $fh, "<", $filename ) or die "Can't open $filename: $!";
	binmode($fh) or die "Can't binmode $filename: $!";
	
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
	
    if    ( $byte_count == 8 ) { $int = hex_to_uint64 unpack "H*", $buffer; }
    elsif ( $byte_count == 4 ) { $int = unpack "N*", $buffer; }
    elsif ( $byte_count == 2 ) { $int = unpack "n*", $buffer; }
    else                       { $int = unpack "H*", $buffer; }
    close ($fh);
    return $int;
}

sub get_int16 {
	
	open( $fh, "<", $filename ) or die "Can't open $filename: $!";
	binmode($fh) or die "Can't binmode $filename: $!";
	
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
	
    $int = unpack "v*", $buffer; }

    close ($fh);
    return $int;
}

sub get_bin {
	open( $fh, "<", $filename ) or die "Can't open $filename: $!\n";
	binmode($fh) or die "Can't binmode $filename: $!\n";
	
    my ( $byte_pos, $byte_count, $bit_pos, $bit_count, $bin, $val );
    ( $byte_pos, $byte_count, $bit_pos, $bit_count ) = @_;
    sysseek( $fh, $byte_pos, SEEK_SET )
      or die "Couldn't seek to byte $byte_pos in $filename: $!\n";
    sysread( $fh, $buffer, $byte_count )
      or die "Could not read to byte $byte_pos in $filename: $!\n";
      
    if ($opt_debug) {
		print "Printing buffer: ";
		printf '[%vd]', $buffer;
		print "\n";
		print "Printing input: \n";
		print "\$byte_pos = $byte_pos\n";
		print "\$byte_count = $byte_count\n";
		print "\$bit_pos = $bit_pos\n";
		print "\$bit_count = $bit_count\n";
	}
	$bin = unpack "b*", $buffer;
	# $val = vec($bin, $bit_pos, $bit_count);
	$val = substr($bin, $bit_pos, $bit_count);

	dbg "\$bin = $bin\n";
	dbg "\$val = $val\n";
    close ($fh);
    return $val;
}

sub get_page {

    my $page = $_[0] or 0;
    my @attr;

    my $offset = SIZE_PAGE * $page;

    # Get FIL Header
	push @attr, get_bytes( ( $offset + 4 ), 4 );    	# 0 page number
    push @attr, get_bytes( $offset, 4 );            	# 1 checksum
    push @attr, get_bytes( ( $offset + 8 ),  4 );   	# 2 prev page
    push @attr, get_bytes( ( $offset + 12 ), 4 );   	# 3 next page
    push @attr, get_bytes( ( $offset + 34 ), 4 );   	# 4 space id
    push @attr, get_bytes( ( $offset + 20 ), 4 );   	# 5 lsn
    push @attr, get_bytes( ( $offset + 24 ), 2 );   	# 6 page type


    # Get Page Header
    push @attr, get_bytes( ( $offset + 38 + 4 ), 2 );   # 7 PAGE_N_HEAP

    # Get Trailer
    push @attr, get_bytes( ( $offset + 16376 ), 4 );    # 8 old-style checksum
    push @attr, get_bytes( ( $offset + 16380 ), 4 );    # 9 low 32 bits of lsn

    push @attr, $offset;    # 10 add the offset in there

    return @attr;
}

sub writepage {
    my ( $byte_pos, $this_page ) = @_;
    my $destfile = "$filename.page." . $this_page;
    
    open OUTF, ">$destfile"
      or die "Can't open $destfile for writing: $!\n";
    binmode OUTF;
    
    open( $fh, "<", $filename ) or die "Can't open $filename: $!";
	binmode($fh) or die "Can't binmode $filename: $!";
	
    sysseek( $fh, $byte_pos, SEEK_SET );
    sysread( $fh, $buffer, 16384 );
    syswrite( OUTF, $buffer );
    close OUTF;
}

sub cur_pos {
    my ($page) = @_;
    SIZE_PAGE * $page;	# Default SIZE_PAGE 16384
}

sub cur_idx_pos {
	my ($page) = @_;
	FIL_PAGE_DATA + (SIZE_PAGE * $page);
}

#
# END Binary Operation Subs
# ----------------------------------



# ----------------------------------
# Subs defined below to represent values retrieved via byte position/length, 
# passed to get_bytes to grab the data from the binary files.
#
# get_bytes at current page offset + offset, with this length
#

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

# INDEX Header data
sub page_n_dir_slots 	{ get_bytes ( cur_idx_pos(@_), 2 ); }		# number of slots in page directory
sub page_heap_top		{ get_bytes ( cur_idx_pos(@_) + 2, 2 ); }	# pointer to record heap top
sub page_n_heap			{ get_bytes ( cur_idx_pos(@_) + 4, 2 ); }	# number of records in the heap ( bit 15=flag: new-style compact page format)
sub page_free			{ get_bytes ( cur_idx_pos(@_) + 6, 2 ); }	# pointer to start of page free record list
sub page_garbage		{ get_bytes ( cur_idx_pos(@_) + 8, 2 ); }	# number of bytes in deleted records
sub page_last_insert	{ get_bytes ( cur_idx_pos(@_) + 10, 2 ); }	# pointer to the last inserted record, or NULL if info has been reset by a delete (for example)
sub page_direction		{ get_bytes ( cur_idx_pos(@_) + 12, 2 ); }	# last insert direction: PAGE_LEFT, ...  
sub page_n_direction	{ get_bytes ( cur_idx_pos(@_) + 14, 2 ); }	# number of consecutive inserts to the same direction
sub page_n_recs 		{ get_bytes ( cur_idx_pos(@_) + 16, 2 ); }	# number of user records on the page
sub page_max_trx_id		{ get_bytes ( cur_idx_pos(@_) + 18, 8 ); }	# highest id of a trx which may have modified a record on the page; trx_id_t; defined only in secondary indexes and in the insert buffer tree

# INDEX Private Data Structure Header
sub page_level			{ get_bytes ( cur_idx_pos(@_) + 26, 2 ); } 	# level of the node in an index tree; the leaf level is the level 0.  This field should not be written to after page creation.
sub page_index_id		{ get_bytes ( cur_idx_pos(@_) + 28, 8 ); } 	# index id where the page belongs. This field should not be written to after	page creation.
sub page_btr_seg_leaf	{ get_bytes ( cur_idx_pos(@_) + 36, 8 ); } 

# INDEX System Records
sub idx_rec_status 	{ 
	dbg "Debugging rec_status on page @_..";
	get_bin ( cur_pos(@_) + SYS_REC_START, 1, 0, 2 ); 
}
sub idx_del_flag	{ 
	dbg "Debugging del_flag on page @_..";
	get_bin ( cur_pos(@_) + SYS_REC_START, 1, 2, 1 ); 
}
sub idx_inf_next	{ get_bytes ( cur_pos(@_) + SYS_REC_START + 97, 2); }
sub idx_sup_next	{ get_bytes ( cur_pos(@_) + SYS_REC_START + 110, 2); }

# Record Data
#sub usr_rec_


# FSEG_HDR, File Segment Pointer Data
sub fseg_hdr_space		{ get_bytes ( cur_idx_pos(@_) + IDX_HDR_SIZE, 4 ); } # Space ID of the Inode
sub fseg_hdr_page_no	{ get_bytes ( cur_idx_pos(@_) + IDX_HDR_SIZE + 4, 4 ); } # Page number of the inode
sub fseg_hdr_offset		{ get_bytes ( cur_idx_pos(@_) + IDX_HDR_SIZE + 8, 2 ); } # Byte offset of the inode

sub fseg_hdr_int_space	{ get_bytes ( cur_idx_pos(@_) + IDX_HDR_SIZE + 10, 4 ); } # Internal non-leaf space ID
sub fseg_hdr_int_page_no { get_bytes ( cur_idx_pos(@_) + IDX_HDR_SIZE + 14, 4 ); } # Internal non-leaf page number
sub fseg_hdr_int_offset { get_bytes ( cur_idx_pos(@_) + IDX_HDR_SIZE + 18, 2 ); } # Internal non-leaf offset


# INODE List Node Data

# FSP Header data
sub fsp_space_id { get_bytes( SIZE_FIL_HEAD, 4 ); }
sub fsp_size { get_bytes( SIZE_FIL_HEAD + 8, 4); }
sub fsp_free_limit { get_bytes ( SIZE_FIL_HEAD + 12, 4); }
sub fsp_space_flags { get_bytes( SIZE_FIL_HEAD + 16, 4); }
sub fsp_frag_n_used { get_bytes ( SIZE_FIL_HEAD + 20, 4); }
sub fsp_free { get_bytes ( SIZE_FIL_HEAD + 24, 4); }
sub fsp_seg_id { get_bytes ( SIZE_FIL_HEAD + 72, 8); }			# First unused segment ID
# sub fsp_seg_inodes_full { get_bytes ( SIZE_FIL_HEAD + 80, )}

#
# END byte position definition subs
# ----------------------------------

sub get_page_type {
    my ($p) = @_;
    foreach my $k ( keys %page_types ) {
        if ( $page_types{$k}{'value'} == $p ) {
            return (
                $k,
                $page_types{$k}{'description'},
                $page_types{$k}{'usage'}
            );
        }
        else {
            next;
        }
    }
}

sub get_rec_type {
	my ($r) = @_;
	#my $v = oct("0b".$r);
	dbg "\nget_rec_type data:\n";
	dbg "\$r = $r\n";
	foreach my $k ( keys %rec_types ) {
		#dbg "Looping through \%rec_types, key: $k\n";
		if ( $k == $r ) {
			return ($rec_types{$k});
		} else {
			next;
		}
	}
}

# ----------------------------------
# Data print/output subs
#

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
    
	
    my ( $nam, $desc, $use ) = get_page_type($type);
       
	#my $checksum = fil_head_checksum($p);

    printf "Page: $offset\n";
    printf "=== HEADER: Page " . fil_head_offset($p) . "\n";
    printf "Byte Start: $cur_pos (" . tohex $cur_pos;
    printf ")\n";
    printf "Page Type: $type\n-- $nam: $desc - $use\n";
    printf "Prev Page: ";
    if   ( $prev == 4294967295 or !$prev ) { printf "Not used.\n"; }
    else                                           { printf "$prev\n"; }
    printf "Next Page: ";
    if   ( $next == 4294967295 or !$next ) { printf "Not used.\n"; }
    else                                           { printf "$next\n"; }
    printf "LSN: $lsn\n";
    printf "Space ID: $id\n";
    printf "Checksum: $checksum\n";
}

sub print_fil_trl {
	my ($p) = @_;
	
	my $csum = fil_trailer_checksum($p);	# Old-style checksum
	my $lsn  = fil_trailer_low32_lsn($p);	# Low 32 bits of LSN
	
	printf "=== TRAILER: Page " . fil_head_offset($p) . "\n";
    printf "Old-style Checksum: $csum\n";
    printf "Low 32 bits of LSN: $lsn\n";
    printf "Byte End: "
      . ( cur_pos($p) + SIZE_PAGE ) . " ("
      . tohex( cur_pos($p) + SIZE_PAGE );
    printf ")\n";
}

sub print_fsp_hdr {
	printf "=== File Header\n";
    printf "Space ID: " . fsp_space_id . "\n";
		verbose "-- Offset 38, Length 4\n";
    printf "Size: " . fsp_size . "\n";
		verbose "-- Offset: " . (38 + 8) . " (" . tohex (38+8) . "), Length 4\n";
	printf "Flags: " . fsp_space_flags . "\n";
	printf "First Unused Segment ID: " . fsp_seg_id . "\n";
	printf "Page Free Limit: " . fsp_free_limit . " (this should always be 64 on a single-table file)\n";
		verbose "-- Offset: 12 (" . tohex(12) . "), Length 4\n";
}

sub print_idx_hdr {
	
	my ($p) = @_;
	
	my $level   = page_level($p);
	my $max_tid = page_max_trx_id($p);
	my $dir     = page_direction($p);
	my $segleaf = page_btr_seg_leaf($p);

	printf "=== INDEX Header: Page " . fil_head_offset($p) . "\n";
	printf "Index ID: " . page_index_id($p) . "\n";
	printf "Node Level: $level\n";
	if ($segleaf) { 
		printf "-- Root-level Leaf Page\n";
	}
	if ($max_tid) {
		printf "Max Transaction ID: $max_tid\n";
	} else {
		printf "-- Secondary Index\n"
	}
	printf "Directory Slots: " . page_n_dir_slots($p) . "\n";
		verbose "-- Number of slots in page directory\n";
	printf "Heap Top: " . page_heap_top($p) . "\n";
		verbose "-- Pointer to record heap top\n";
	printf "Records in Page: " . page_n_recs($p) . "\n";
	printf "Records in Heap: " . page_n_heap($p) . "\n";
		verbose "-- Number of records in heap\n";
	printf "Start of Free Record List: " . page_free($p) . "\n";
	printf "Garbage Bytes: " . page_garbage($p) . "\n";
		verbose "-- Number of bytes in deleted records.\n";
	printf "Last Insert: " . page_last_insert($p) . "\n";
	printf "Last Insert Direction: $dir";
		if ($dir == PAGE_LEFT) {print " - Left";}
		if ($dir == PAGE_RIGHT) {print " - Right";}
		if ($dir == PAGE_SAME_REC) {print " - Page Same Record";}
		if ($dir == PAGE_SAME_PAGE) {print " - Page Same Page";}
		if ($dir == PAGE_NO_DIRECTION) {print " - No Direction";}
	nl;
	printf "Inserts in this direction: " . page_n_direction($p) . "\n";
		verbose "-- Number of consecutive inserts in this direction.\n";
}

sub print_fseg_hdr {
	
	my ($p) = @_;
	
	my $sid = fseg_hdr_space($p); # space id
	my $ipn = fseg_hdr_page_no($p); # inode page number
	my $off = fseg_hdr_offset($p); # inode offset
	
	my $int_sid = fseg_hdr_int_offset($p); 
	my $int_ipn = fseg_hdr_int_page_no($p);
	my $int_off = fseg_hdr_int_offset($p);
	
	my $inc = IDX_HDR_SIZE; # increment
	
	printf "=== FSEG_HDR - File Segment Header: Page " . fil_head_offset($p) . "\n";
	printf "Inode Space ID: $sid\n";
		verbose "-- Offset: " . ( cur_idx_pos($p) + $inc ) . " (" . tohex ( cur_idx_pos($p) + $inc ) . "), Length: 4\n";
	printf "Inode Page Number: $ipn\n";
		verbose "-- Offset: " . ( cur_idx_pos($p) + $inc + 4 ) . " (" . tohex ( cur_idx_pos($p) + $inc + 4 ) . "), Length: 4\n";
	printf "Inode Offset: $off\n";
		verbose "-- Offset: " . ( cur_idx_pos($p) + $inc + 8 ) . " (" . tohex ( cur_idx_pos($p) + $inc + 8 ) . "), Length: 2\n";
	if (!page_level($p)) {
		printf "Non-leaf Space ID: $int_sid\n";
			verbose "-- Offset: " . ( cur_idx_pos($p) + $inc + 10 ) . " (" . tohex ( cur_idx_pos($p) + $inc + 10 ) . "), Length: 2\n";
		printf "Non-leaf Page Number: $int_ipn\n";
			verbose "-- Offset: " . ( cur_idx_pos($p) + $inc + 14 ) . " (" . tohex ( cur_idx_pos($p) + $inc + 14 ) . "), Length: 2\n";
		printf "Non-leaf Offset: $int_off\n";
			verbose "-- Offset: " . ( cur_idx_pos($p) + $inc + 18 ) . " (" . tohex ( cur_idx_pos($p) + $inc + 18 ) . "), Length: 2\n";
	}
	
}

sub print_sys_rec {
	my ($p) = @_;
	my ($name, $value, $offset, $len, $type);
	# my $rec_status = idx_rec_status($p);
	my %recs = (
		'REC_STATUS' => {
			'name'		=> 'Index Record Status',
			'value'		=> idx_rec_status($p), 
			'offset'	=> cur_pos(@_) + SYS_REC_START,
			'len'		=> length(idx_rec_status($p))
		},
		'REC_DELETE' => {
			'name'		=> 'Deleted',
			'value'		=> idx_del_flag($p),
			'offset'	=> cur_pos(@_) + SYS_REC_START,
			'len'		=> length(idx_del_flag($p))
		},
		'REC_INF_NEXT' => {
			'name'		=> 'Next Record Offset (Infimum)',
			'value'		=> idx_inf_next($p),
			'offset'	=> cur_pos(@_) + SYS_REC_START + 97,
			'len'		=> length(idx_inf_next($p))
		},
		'REC_SUP_NEXT' => {
			'name'		=> 'Next Record Offset (Supremum)',
			'value'		=> idx_sup_next($p),
			'offset'	=> cur_pos(@_) + SYS_REC_START + 110,
			'len'		=> length(idx_sup_next($p))
		}
	);
	
	dbg "\nDumping \%recs..\n";
	dbg Dumper (\%recs);
	dbg "\n";
	
	printf "=== INDEX System Records: Page " . fil_head_offset($p) . "\n";
	foreach my $k ( keys %recs ) {
        #if ( $recs{$k}{'value'} == $p ) {
		$name 	= $recs{$k}{'name'};
		$value 	= $recs{$k}{'value'};
		$offset = $recs{$k}{'offset'};
		$len	= $recs{$k}{'len'};
		if ($k eq "REC_STATUS") { $type = get_rec_type(oct("0b".$value)); }
		
		dbg "\$name = $name\n";
		dbg "\$value = $value\n";
		dbg "\$offset = $offset\n";
		dbg "\$len = $len\n";
		
		printf "$name: $value";
		if ($type) { printf " - (Decimal: " . oct("0b".$value) . ") $type"; }
		nl;
			verbose "-- Offset $offset, Length $len bits\n";
    }
	
}

sub list_page {
	my ($p) = @_;
	my $type = fil_head_page_type($p);
	my $realp = fil_head_offset($p);
	my ( $nam, $desc, $use ) = get_page_type($type);
	my $idxid = page_index_id($p);
	my $cur_pos 	= cur_pos( fil_head_offset($p) );
	
	print "-- Page $p - $nam: $desc,";
	if ($type == '17855') { print " Index ID: $idxid,"; }
	print " Byte Start: $cur_pos (" . tohex($cur_pos) . ")\n"; 
}

sub process_page {
	my $page_start = SIZE_PAGE * $set_page;
	if ( $page_start < $file_size and looks_like_number $set_page) {
		if ($opt_chop) {
			print "Writing page $set_page to $filename.page.$set_page..\n";
			writepage( cur_pos($set_page), $set_page );
			exit 0;
		}
		print_fil_hdr( $set_page );
		nl;
		print_fil_trl( $set_page );
	}
	else {
		print "Invalid page.\n";
	}
}

sub process_pages {
	
	my ($p) = @_;
	$set_type //= 0;
	
	unless ($set_type eq 'INDEX' or $p) {
		print_fsp_hdr;
	}
	
	if ($p) { 
		my $page_start = SIZE_PAGE * $p; 
		my $type = fil_head_page_type($p);
		if ( $page_start < $file_size and looks_like_number $p) {
			if ($opt_chop) {
				print "Writing page $p to $filename.page.$p..\n";
				writepage( cur_pos($p), $p );
				exit 0;
			}
			nl;
			print_fil_hdr( $p );
			if ($type == '17855') {
				nl;
				print_idx_hdr($p);
				nl;
				print_fseg_hdr($p);
				nl;
				print_sys_rec($p);
			}
			nl;
			print_fil_trl( $p );
		}
		exit 0;
	}
		

	
	for ( $i = 0 ; $i < $page_count ; $i++ ) {
			
		my $type = fil_head_page_type($i);
		
		if ($set_type) {
			unless (uc $set_type eq 'INDEX' and $type == '17855') { next; }
		}
		if ($opt_chop) {
			writepage( cur_pos($i), $i );
		}			
		my $this_csum = fil_head_checksum($i);
		if ($this_csum) {		
			unless ($set_type  eq 'INDEX') { 
				nl; 
				print_fil_hdr($i); 
			}
			#if ( $type == '17855' ) {
			if ($type == '17855') {
				nl;
				print_idx_hdr($i);
				nl;
				print_fseg_hdr($i);
				nl;
				print_sys_rec($i);
			}
			unless ($set_type  eq 'INDEX') { 
				nl; 
				print_fil_trl($i);	}
		}
	}
}

# 
# END Data print/output subs
# ----------------------------------

sub csum_calc {
	
	my ($p) = @_;
	
	my $lastt 	= 0;
	my $ct		= $p;
	
	open( $fh, "<", $filename ) or die "Can't open $filename: $!";
	binmode($fh) or die "Can't binmode $filename: $!";
	
	while (!eof($fh)) {
		my $mod = 0;
		my $lsn = fil_head_lsn($p);
		my $lsn_field = fil_trailer_low32_lsn($p);
		if ($lsn != $lsn_field) {
			printf STDERR ("page %lu invalid (fails log sequence number check)\n", $ct);
			printf("page %lu: log sequence number: first = 0x%08X; second = 0x%08X\n", $ct, $lsn, $lsn_field);
		}
		my $csum = buf_calc_page_new_checksum($p);
		my $csum_field = fil_head_checksum($p);
	}
}

if ($opt_help) {
	usage;
	exit 0;
}

if ($file) { 
	$filename = $file; 
} elsif ($ARGV[-1]) { 
	$filename = $ARGV[-1]; 
} else {
	print STDERR "Warning: Filename invalid or no filename specified.\n";
}

unless (!$filename) {
	
	if ( !-e $filename ) {
		print STDERR "File path provided does not exist.\n";
		exit;
	}
	
	if ( !-r $filename ) {
		print STDERR "File path provided is not readable.\n";
		exit;
	}
	
	$file_size  = -s $filename or die "Could not retrieve size of $filename: $!";
	$page_count = $file_size / SIZE_PAGE;
	
}

# ----------------------------------
# Toggled routines
#

if ($opt_records) {
	open( $fh, "<", $filename ) or die "Can't open $filename: $!";
	binmode($fh) or die "Can't binmode $filename: $!";
	
	my $log_file_size = -s $filename;
	my $block_size    = SIZE_LOG_BLOCK;
	my $block_count   = $log_file_size / $block_size;

	close $fh;
	exit;
}

if ($opt_list) {
	for ($i = 0 ; $i < $page_count ; $i++ ) {
		list_page $i;
	}
	exit 0;
}

if ($opt_chop) {
    print "Splitting into page files..\n";
}

if ($set_page) {
	process_pages($set_page);
} else {
	process_pages;
}

nl;
# 
# END Toggled routines
# ----------------------------------
