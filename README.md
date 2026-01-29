# fasts3

Quickly list the object paths and their byte sizes (tab delimited)
in an S3 bucket path.

```
go install .

fasts3 bucket/path/ > output
```

There is also a `--benchmark` option that tries 3 different
listing methods and only outputs (on STDERR) timings.

