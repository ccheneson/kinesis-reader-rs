## kinesis-reader-rs, a CLI tool that reads AWS Kinesis Stream in Rust.

This project builds an executable that you can use to read a kinesis stream (considering that the items in the streams are in JSON).

It can be useful to see how the data that is sent to the stream is structured.  

### Executable
Check out the project and run from a terminal
```aidl
cargo build --release
```

To run the cli
```aidl
kinesis-reader-rs my-stream
```

