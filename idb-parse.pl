#!/usr/bin/perl

use bytes;

use strict 'vars';
use warnings;

#use diagnostics;
use Data::Dumper qw(Dumper);

use Fcntl qw(:seek);
use Getopt::Long qw(:config gnu_getopt);
use Scalar::Util qw(looks_like_number);
use Math::Int64 qw( :native_if_available int64 hex_to_uint64 uint64_to_hex );

use constant {
    SIZE_FIL_HEAD    		=> '38',		# Page Header Size - Default 38
    SIZE_FIL_TRAILER 		=> '8',			# Page Trailer Size - Default: 8
    SIZE_PAGE        		=> '16384',		# Page Size - Default: 16384
    SIZE_LOG_BLOCK	 		=> '512',		# Log Block Size - Default: 512
    SIZE_LOG_BLOCK_HDR		=>  14,			# Log Block Header Size - Default: 14
	LOG_CHECKPOINT_GROUPS 	=> '32',  		# Maximum number of log group checkpoints
	LOG_MAX_N_GROUPS		=> '32',		# Maximum number of log groups in log_group_struct::checkpoint_buf
	LOG_BLOCK_FLUSH_BIT_MASK => 2147483648  # (0x80000000UL) Mask used to get the highest bit in the preceding field
};

my ( $fh, $filename, $hex, $buffer );

our $POS_PAGE_BODY   			= SIZE_FIL_HEAD;
our $POS_FIL_TRAILER 			= SIZE_PAGE - SIZE_FIL_TRAILER;
our $LOG_CHECKPOINT_ARRAY_END 	= LOG_CHECKPOINT_GROUPS + (LOG_MAX_N_GROUPS * 8); # Defaults to 32 + (32 * 8) = 288

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
	$opt_log,
	$opt_debug,
	$opt_quiet,
	$opt_verbose,
	$opt_vv,
	$opt_noempty,
	$opt_records,
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
    'i'   => \$opt_ibdata,
    'l'	  => \$opt_log,
    'v'	  => \$opt_verbose,
    'vv'  => \$opt_vv,
    'e'	  => \$opt_noempty,
    'r'	  => \$opt_records
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
         [-f[ile]] <file>        	Path to InnoDB data file. (Default behavior)
         [-p[age]] <page #>		 Specify a single page to get information from.
         [-i[bdata]] <file>		 Specify an InnoDB system data file (eg. ibdata1) to get information from.
         [-v[erbose]]            	Displays additional information.
         [-d[ebug]]      		 Displays debug information.
         [-h[elp]]               	Displays this Usage information.
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
    
    20 => 'UNDO_INSERT',			# Identifies data manipulation statements.
									# 1st byte: 0x14
									# Stores affected table ID#, ID for statement type, and additional information (depending on statement type)

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
    38 => 'COMP_REC_INSERT',		# Insertion of new record - 1st byte: 0x26
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

# ----- Define data manipulation types
my @manip_types = {
	hex 0x0B => 'INSERT',
	hex 0x1C => 'UPDATE',
	hex 0x0E => 'DELETE',
};

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

#
# Utility Subs
# ----------------------------------

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
# Binary Operation Subs
# ----------------------------------

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
	
    if    ( $byte_count == 8 ) { $int = hex_to_uint64 unpack "H*", $buffer; }
    elsif ( $byte_count == 4 ) { $int = unpack "N*", $buffer; }
    elsif ( $byte_count == 2 ) { $int = unpack "n*", $buffer; }
    else                       { $int = unpack "H*", $buffer; }
    return $int;

}

# Set byte offset / current position
sub cur_pos {
    my ($page) = @_;
    $page_size * $page;
}

sub cur_log_pos {
	my ($c) = @_;
	SIZE_LOG_BLOCK * $c;
}

sub cur_rec_pos {
	my ($r) = @_;
	cur_log_pos($r) + SIZE_LOG_BLOCK_HDR;
}

# 
# Subs defined below to represent values retrieved via byte position/length, 
# passed to get_bytes to grab the data from the binary files.
# ----------------------------------

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
sub log_flush_lsn		{ get_bytes( ( cur_log_pos(@_) + 12 ), 4 ); }

# Log File Header
sub log_group_id		{ get_bytes( 0, 4 ); }
sub log_file_start_lsn	{ get_bytes( 4, 8 ); }	
sub log_file_num		{ get_bytes( 12, 4 ); }		# LOG_FILE_NO - 4-byte archived log file number; this field is only defined in an archived log file.
sub log_created_by		{ get_bytes( 16, 32 ); }	# LOG_FILE_WAS_CREATED_BY_HOT_BACKUP

# Log File Checkpoint
sub log_checkpoint_num	{ get_bytes( cur_log_pos(@_), 8 ); }
sub log_checkpoint_lsn	{ get_bytes( (cur_log_pos(@_) + 8), 8 ); }
sub log_checkpoint_log_offset	{ get_bytes( (cur_log_pos(@_) + 16), 4 ); }
sub log_checkpoint_log_buf_size	{ get_bytes( (cur_log_pos(@_) + 20), 4 ); }
sub log_checkpoint_archived_lsn	{ get_bytes( (cur_log_pos(@_) + 24), 8 ); }
sub log_checkpoint_checksum_1	{ get_bytes( (cur_log_pos(@_) + $LOG_CHECKPOINT_ARRAY_END), 4 ); } 		# Defaults would be: 512 + 288 = 800
sub log_checkpoint_checksum_2	{ get_bytes( (cur_log_pos(@_) + ($LOG_CHECKPOINT_ARRAY_END + 4)), 4 ); }
sub log_checkpoint_fsp_free_limit 	{ get_bytes( (cur_log_pos(@_) + ($LOG_CHECKPOINT_ARRAY_END + 8)), 4 ); }
sub log_checkpoint_fsp_magic_num	{ get_bytes( (cur_log_pos(@_) + ($LOG_CHECKPOINT_ARRAY_END + 12)), 4 ); }
sub log_checkpoint_size				{ get_bytes( (cur_log_pos(@_) + ($LOG_CHECKPOINT_ARRAY_END + 16)), 4 ); }

# Log Block Header
sub log_block_hdr_num			{ get_bytes( cur_log_pos(@_), 4); }
sub log_block_hdr_data_len 		{ get_bytes( (cur_log_pos(@_) + 4), 2); }	# Number of bytes of log written to this block.
sub log_block_first_rec_group 	{ get_bytes( (cur_log_pos(@_) + 6), 2); }	# Offset of first start of an MTR log ercord in this log block. 0 if none. If same as LOG_BLOCK_HDR_DATA_LEN, means that first rec group has not yet been added to this log block - if it does, it will start @ this offset.
sub log_block_checkpoint_num	{ get_bytes( (cur_log_pos(@_) + 8), 4); }
sub log_block_hdr_size			{ get_bytes( (cur_log_pos(@_) + 12), 2); }

# Log Block Trailer
sub log_block_checksum			{ get_bytes( (cur_log_pos(@_) + SIZE_LOG_BLOCK) - 8, 4); }
sub log_block_trl_size			{ get_bytes( (cur_log_pos(@_) + SIZE_LOG_BLOCK) - 4, 4); }

# Log Block Records
sub log_rec_entry_type			{ get_bytes( (cur_rec_pos(@_)), 1); }

sub print_log_record {
	my ($b) = @_;
	printf "Log Record\n";
	hr;
	printf "Log Entry Type: " . log_rec_entry_type($b) . "\n";
}

sub print_log_block {
	
	my ($b) = @_; # Get block number
	my ($blocknum, $flush);
	
	my $mtr_start 	= log_block_first_rec_group($b);
	my $data_len 	= log_block_hdr_data_len($b);
	my $byte_start	= cur_log_pos($b);
	my $byte_end	= $byte_start + 512;
	my $hdr_size	= log_block_hdr_size($b);
	my $cnum		= log_block_checkpoint_num($b);
	
	# Check to see if flush bit mask is set. If so, extract the real block number from the returned value and toggle $flush.
	if (log_block_hdr_num($b) > LOG_BLOCK_FLUSH_BIT_MASK) {
		$blocknum = log_block_hdr_num($b) - LOG_BLOCK_FLUSH_BIT_MASK;
		$flush = 1;
	} else {
		$blocknum = log_block_hdr_num($b);
	}
	
	if  ($opt_noempty and !$cnum) {
		return;
	} else {
		printf "Log Block\n";
		hr;
		printf "HEADER\n";
		verbose "Byte Start: $byte_start (" . tohex($byte_start) . ")\n";
		printf "Block Number";
			if ($flush) {
				printf ": $blocknum - Flush bit mask is set; first block in log flush write segment.";
			} else {
				printf ": " . log_block_hdr_num($b);
			}
			vv "\n-- (LOG_BLOCK_HDR_NO - off 0, len 4)";
			nl;
			if ($data_len == 512) { 
				vv "Block Length";			
				vv ": $data_len (Fully written)";
				vv "\n-- (LOG_BLOCK_HDR_DATA_LEN - off 4, len 2)";
				vv "\n";
			} else {
				printf "Block Length";
				printf ": $data_len (Not fully written)";
				vv "\n-- (LOG_BLOCK_HDR_DATA_LEN - off 4, len 2)";
				nl;	
			}
			
		if ($mtr_start) { 
			printf "Starting MTR Log Record Offset";		
				printf ": $mtr_start";
				vv "\n-- (LOG_BLOCK_FIRST_REC_GROUP - off 6, len 2)";
				nl;
		}
		printf "Block Checkpoint Number";			
			printf ": " . log_block_checkpoint_num($b);
			vv "\n-- (LOG_BLOCK_CHECKPOINT_NO - off 8, len 4)";
			nl;
		printf "Header Data Size";	
			if (!$hdr_size) {
				printf ": No data written yet.";
			} else {
				printf ": " . log_block_hdr_size($b);
			}
			vv "\n-- (LOG_BLOCK_HDR_SIZE - off 12, len 2)";
			nl;
		nl;
		printf "TRAILER\n";
		printf "Block Checksum";
			printf ": " . log_block_checksum($b);
			vv "\n-- (LOG_BLOCK_CHECKSUM - offset 512 - 8, len 4)";
			nl;
		printf "Trailer Size";
			printf ": " . log_block_trl_size($b);
			vv "\n-- (LOG_BLOCK_TRL_SIZE - offset 512 - 4, len 4)";
			nl;
		verbose "Byte End: $byte_end (" . tohex($byte_end) . ")\n";
		hr;
	}
}

sub get_page {

    my $page = $_[0] or 0;
    my @attr;

    $offset = $page_size * $page;

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

sub print_log_fil_hdr {
	
	printf "Log File Header - Block 0:\n";
	hr; 
	printf "Log Group ID";		
		printf ": " . log_group_id;
		vv "\n-- (LOG_GROUP_ID - off 0, len 4)";
		nl;
	printf "Starting LSN";
		printf ": " . log_file_start_lsn;
		vv "\n-- (LOG_FILE_START_LSN - off 4, len 8)";
		nl;
	printf "Log File Number";		
		printf ": " . log_file_num;
		vv "\n-- (LOG_FILE_NO - off 12, len 4)";
		nl;
#	printf "Created by Hot Backup?";
#		if ($opt_verbose) { printf " (LOG_FILE_WAS_CREATED_BY_HOT_BACKUP - off 16, len 32)"; }
#		printf ": " . log_created_by . "\n";
	hr;
}

sub print_log_checkpoint {
	
	my ($b) = @_; # Set block number
	
	printf "Log Checkpoint:\n";
	hr;
	printf "Checkpoint Number";
		printf ": " . log_checkpoint_num($b);
		vv "\n-- (LOG_CHECKPOINT_NO - off 512, len 8)";
		nl;
	printf "Checkpoint LSN";
		printf ": " . log_checkpoint_lsn($b);
		vv "\n-- (LOG_CHECKPOINT_LSN - off 512 + 8, len 8)";
		nl;
	printf "Checkpoint Offset";
		printf ": " . log_checkpoint_log_offset($b);
		vv "\n-- (LOG_CHECKPOINT_OFFSET - off 512 + 16, len 4)";
		nl;
	printf "Checkpoint Log Buffer Size";
		printf ": " . log_checkpoint_log_buf_size($b);
		vv "\n-- (LOG_CHECKPOINT_LOG_BUF_SIZE - off 512 + 20, len 4)";
		nl;
	printf "Archived LSN";
		
		if (log_checkpoint_archived_lsn($b) == 18446744073709551615) {
			printf ": UNIV_LOG_ARCHIVE not activated.";
		} else {
			printf ": " . log_checkpoint_archived_lsn($b);
		}
		vv "\n-- (LOG_CHECKPOINT_ARCHIVED_LSN - off 512 + 24, len 8)";
		nl;
	printf "Checksum 1";	
		printf ": " . log_checkpoint_checksum_1($b);
		vv "\n-- (LOG_CHECKPOINT_CHECKSUM_1 - off 512 + 288 (calculated by log_checkpoint_group_array + [log_max_n_groups*8]), len 4)";
		nl;
	printf "Checksum 2"; 	
		printf ": " . log_checkpoint_checksum_2($b);
		vv "\n-- (LOG_CHECKPOINT_CHECKSUM_2 - off 512 + (288 + 4), len 4)";
		nl;
	printf "Checkpoint Size"; 
		printf ": " . log_checkpoint_size($b);
		vv "\n-- (LOG_CHECKPOINT_SIZE - off 512 + (288 + 16), len 4)";
		nl;
	hr;
}

#-- TOGGLED MODES
if ($opt_records) {
	open( $fh, "<", $filename ) or die "Can't open $filename: $!";
	binmode($fh) or die "Can't binmode $filename: $!";
	
	$log_file_size = -s $filename;
	my $block_size = SIZE_LOG_BLOCK;
	my $block_count = $log_file_size / $block_size;
}

# Parse ib_logfile:
if ($opt_log) {
	
	open( $fh, "<", $filename ) or die "Can't open $filename: $!";
	binmode($fh) or die "Can't binmode $filename: $!";
	
	my $log_file_size 	= -s $filename;	
	my $block_size		= SIZE_LOG_BLOCK; 					# Default 512 - Change in constants
	my $block_count 	= $log_file_size / $block_size; 
	
	printf "$filename:\n";
	hr;
	print_log_fil_hdr;			# Log file header output
	print_log_checkpoint(1); 	# Log Checkpoint 1 output
	print_log_checkpoint(3);	# Log Checkpoint 2 output (2nd block skipped as padding)
	
	for ($i = 4; $i < $block_count; $i++) {
		print_log_block($i);
	}
	exit;	
}

if ($find_page) {
    my $datadir = `mysqld --verbose --help 2> /dev/null | grep \"datadir\\s\" | sed 's/.*\\s//'`;
    print "Datadir: $datadir\n";
    chomp($datadir);
    $datadir =~ s/\/$//;
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
