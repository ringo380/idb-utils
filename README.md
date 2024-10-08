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

The idb-corrupter.pl script is used to corrupt pages in a table for demonstration or testing purposes. It provides various options to specify how and where the corruption should occur.

**Usage:**

```bash
perl idb-corrupter.pl [-f <file>] [-p <page #>] [-b <bytes>] [-v] [-d] [-k] [-r] [-o <offset>] [-h]
```

**Options:**

```bash
- -f, --file <file>: Path to the InnoDB data file. This is a required option unless specified otherwise.
- -p, --page <page #>: Specify the page number to corrupt. If not specified, a random page will be chosen.
- -b, --bytes <bytes>: Sets the amount of bytes to corrupt. Default is 1 byte.
- -v, --verbose: Displays additional information during execution.
- -d, --debug: Displays debug output for troubleshooting.
- -k: Corrupt the page's FIL header area.
- -r, --records: Corrupt the record area specifically.
- -o, --offset <offset>: Set the exact byte offset you'd like to corrupt.
- -h, --help: Displays usage information.
```

**Examples:**

1. Corrupt a specific page in a file:
   perl idb-corrupter.pl -f city2.ibd -p 5

2. Corrupt a random page with verbose output:
   perl idb-corrupter.pl -f city2.ibd -v

3. Corrupt the FIL header area of a specific page:
   perl idb-corrupter.pl -f city2.ibd -p 3 -k

4. Corrupt a specific number of bytes at a given offset:
   perl idb-corrupter.pl -f city2.ibd -o 100 -b 10

Notes:

- Ensure you have the necessary permissions to modify the specified file.
- Use this script with caution, as it will intentionally corrupt data.

### idb-findtsid.pl

This script helps in locating tablespace IDs within a database directory. It provides options for listing and setting database paths.

## Testing

The project includes a test script located in the IdbHelpers/t directory. You can run the tests using:

```bash
perl IdbHelpers/t/001_load.t
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.