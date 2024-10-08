# IDB Utils

IDB Utils is a collection of Perl scripts designed to assist with various database-related tasks. These utilities provide functionalities such as parsing, finding pages, and handling configurations for database files.

## Table of Contents

- [IDB Utils](#idb-utils)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Scripts](#scripts)
    - [idb-liveinfo.pl](#idb-liveinfopl)
    - [idb-parse.pl](#idb-parsepl)
    - [idb-findpage.pl](#idb-findpagepl)
    - [idb-corrupter.pl](#idb-corrupterpl)
    - [idb-findtsid.pl](#idb-findtsidpl)
  - [Testing](#testing)
  - [Contributing](#contributing)

## Installation

To use these scripts, ensure you have Perl installed on your system. Clone the repository to your local machine:

```bash
git clone https://github.com/ringo380/idb-utils.git
```

## Usage

Each script has its own set of options and functionalities. You can run them using Perl:

```bash
perl scriptname.pl [options]
```

For detailed usage instructions, refer to the help option for each script (usually -h or --help).

## Scripts

### idb-liveinfo.pl

This script is used to parse configuration files and extract relevant information. It reads a configuration file and creates a hash of name-value pairs.

### idb-parse.pl

This script reads a G2++ file and converts it into XML. It offers various options for customization, such as verbosity and debugging.

### idb-findpage.pl

This script is designed to find specific pages within a database file. It includes options for specifying the data directory, page size, and more.

### idb-corrupter.pl

This script is used to manipulate database files, potentially for testing corruption scenarios. It includes options for setting the mode, multiplier, and offset.

### idb-findtsid.pl

This script helps in locating tablespace IDs within a database directory. It provides options for listing and setting database paths.

## Testing

The project includes a test script located in the IdbHelpers/t directory. You can run the tests using:

```bash
perl IdbHelpers/t/001_load.t
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.