Kitchen
=======

Kitchen is an open source application for real time synchronization of files across multiple computers.  It depends on rust nightly in order to function.

# Usage

On the server
```
cargo run --bin server <path_to_folder>
```

On the client
```
cargo run --bin client -u <server_address> <path_to_folder>
```
