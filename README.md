# Custom Java DBMS Engine

A relational database management system built from scratch in Java.

## Features
- Table creation and deletion
- Record insertion and conditional selection
- Pointer-based page access
- Multi-layer architecture (DB → Table → Page)
- Persistent storage across sessions
- Traceable metadata for query history

## Architecture
The engine is structured in three layers:
- **DB layer** — manages multiple tables
- **Table layer** — handles schema and record management
- **Page layer** — handles physical storage and pointer-based access

## Tech stack
Java · File I/O · Object Serialization

## About
Built as a project for the Database course at the
German University in Cairo.
