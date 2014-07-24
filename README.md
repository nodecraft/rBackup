rBackup
=======

Command line RethinkDB to JSON backup and restore script built in Node.js. This does not restore secondary indexs, sharing, or replication. This is only a method to store the data as raw JSON to hard backups.

    npm install rbackup -g


```
Usage: rbackup [options]

  Options:

    -h, --help                            output usage information
    -V, --version                         output the version number
    -h, --host [value]                    Hostname
    -p, --port [value]                    Port
    -d, --db, --database [value]          Database Name
    -a, --auth, --authentication [value]  Authentication
    -f, --folder [value]                  Folder
    -c, --cwd [value]                     Current Working Directory
    -i, --import                          Set functionality to Import
```

ReThinkDB Versioning
--------------------

While this script does a good job of importing data, it does not manage the RethinkDB driver version. You may need to change the version of RethinkDB's driver used.
