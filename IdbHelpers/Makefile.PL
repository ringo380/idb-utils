

use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME         => 'IdbHelpers',
    VERSION_FROM => 'lib/IdbHelpers.pm', # finds \$VERSION
    AUTHOR       => 'Ryan Robson (a.u.thor@a.galaxy.far.far.away)',
    ABSTRACT     => 'InnoDB definitions and functions to assist in structure specifications',
    PREREQ_PM    => {
                     'Test::Simple' => 0.44,
                    },
);
