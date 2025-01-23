This is a initial import into a SQL Server database from public resources. 

### Run Import

You need to prepare `.env` file with something similar to the following content:
```txt
DB_USERNAME=rvvi_dbt
DB_PASSWORD=xxx
DB_SERVER=dbsys.cs.vsb.cz\sqldb
DB_DATABASE=RVVI_dbt
DB_SCHEMA=staging
```

Once your `.env` file is ready, you can run the import script:
```bash
make -f MakeFile run
```

The import script will download the data from the public resources and import it into the database.
