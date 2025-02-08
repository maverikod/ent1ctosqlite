# ent1ctosqlite

![Tests](https://github.com/maverikod/ent1ctosqlite/workflows/Tests/badge.svg)

Version 0.1.1

A tool for converting 1C:Enterprise configuration files extracted from the configurator using the "Upload to files" command into a SQLite database

## Installation

pip install ent1ctosqlite

## Usage

ent1ctosqlite path/to/config.zip

### Command Line Arguments

- `zip_path` - path to the configuration export zip archive
- `-o, --output` - path to the extraction directory (default: temp)
- `-d, --database` - path to the SQLite database file (default: vcv_parser.db)
- `--log-file` - save log to file
- `--debug` - enable debug mode
- `--check-db` - check database integrity

## Development

1. Clone the repository
2. Create a virtual environment
3. Install development dependencies
4. Run testshttps://github.com/maverikod/ent1ctosqlite.git

git clone

cd ent1ctosqlite

python -m venv venv

source venv/bin/activate # On Windows: venv\Scripts\activate

pip install -e ".[dev]"

pytest

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT

## Authors

- Your Name (@yourusername)

## Acknowledgments

- Thanks to all contributors
- Inspired by the need for better 1C:Enterprise configuration analysis tools
