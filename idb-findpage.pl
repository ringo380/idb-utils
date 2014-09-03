#!/usr/bin/perl

use warnings;
use Fcntl qw(:seek);
use Getopt::Long qw(:config gnu_getopt);
use Scalar::Util qw(looks_like_number);

my ( $fh, $filename, $hex, $buffer, $offset, $page_size, $page_count,
    $file_size );

my $help    		= '';
my $datadir 		= '';
my $find_page   	= '';
my $checksum		= '';
my $spaceid			= '';

GetOptions(
    'h'   => \$help,
    'd=s' => \$datadir,
    'p=i' => \$find_page,
	'c=i' => \$checksum,
	's=i' => \$spaceid
) or die("Could not get options.\n");

unless ($datadir) { 
	$datadir = `mysqld --verbose --help 2> /dev/null | grep "datadir\\s" | sed 's/.*\\s//'` 
		or die "Could not determine data directory. Please specify a data directory manually with the '-d' flag.\n";
}

sub get_bytes {
    my ( $byte_pos, $byte_count );
    ( $byte_pos, $byte_count ) = @_;
    sysseek( $fh, $byte_pos, SEEK_SET )
      || die "Couldn't seek to byte $byte_pos in $filename";
    sysread( $fh, $buffer, $byte_count )
      || die "Could not read to byte $byte_pos in $filename";
    $hex = unpack "H*", $buffer;
    return hex $hex;
}

sub get_page {

    my $page = $_[0];
    $page //= 0;
    my @attr;

    $offset = $page_size * $page;

    # Get Header
    push @attr, &get_bytes( ( $offset + 4 ), 4 );    # 0 page number
    push @attr, &get_bytes( $offset, 4 );            # 1 checksum
    push @attr, &get_bytes( ( $offset + 34 ), 4 );   # 2 space id
    push @attr, &get_bytes( ( $offset + 20 ), 4 );   # 3 lsn
    push @attr, &get_bytes( ( $offset + 24 ), 2 );   # 4 page type

    # Page data
    push @attr,
      &get_bytes( ( $offset + 38 + 8 ), 4 )
      ;    # 5 PAGE_N_HEAP - amount of records in page

    # Get Trailer
    push @attr, &get_bytes( ( $offset + 16376 ), 4 );    # 6 old-style checksum
    push @attr, &get_bytes( ( $offset + 16380 ), 4 );    # 7 low 32 bits of lsn

    push @attr, $offset;    # 8 add the offset in there

    return @attr;
}

if ($find_page) {
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
            @tblattr = &get_page($i);
            if ( $tblattr[0] == $find_page and $tblattr[1] == $checksum or $tblattr[2] == $spaceid ) {
                print "Found page $find_page in $tblfile.\n";
                exit;
            }
        }
        close($fh);
    }
    exit;
}

