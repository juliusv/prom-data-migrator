# prom-data-migrator

**NOTE**: This tool is **DEPRECATED** in favor of [https://gitlab.com/gitlab-org/prometheus-storage-migrator](https://gitlab.com/gitlab-org/prometheus-storage-migrator), which is both more memory-efficient and correct (it also migrates series that don't have particular labels like `instance`).

Tool to migrate Prometheus 1.x data directories to the 2.0 format.

## Prerequisites

Golang 1.9 or higher

## Build

```
go build
```

## Run

```
./prom-data-migrator -v1-dir=./data-old -v2-dir=./data-new 2> migration.log
```

## Flags

```
./prom-data-migrator -h
```
