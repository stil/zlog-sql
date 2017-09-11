# zlog-sql
MySQL/SQLite logging plugin for ZNC IRC bouncer written in Python 3


## Quick start
1. Copy `zlog_sql.py` to `~/.znc/modules/zlog_sql.py`.
2. In Webadmin, open the list of Global Modules.
3. Make sure `modpython` is enabled.
4. Enable module `zlog_sql` and set its argument.

![Screenshot](docs/webadmin_modules.png)

For MySQL, set module argument matching following format:
```
mysql://username:password@localhost/database_name
```

For SQLite use following string format:
```
sqlite://home/user/logs.sqlite
```

or simply leave out the path
```
sqlite
```
in this case, logs are going to be written to the default path `~/.znc/moddata/zlog_sql/logs.sqlite`.

5. Save changes. SQL table schema is going to be created automatically.
