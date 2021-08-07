# CSV IMPORTER TO SQLITE Database

### Arguments:
* -f --force: Recreate database if it exists
* -p --path: Path to CSV files
* -s --separator: Defines CSV separator (default = ;)
* -n --name: Database file name


### Docker Run
docker run --rm -v $(pwd):/app/csv csvimportersqlite -p /app/csv -f -s ";" -n "stocks_database"