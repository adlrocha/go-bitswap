# Compile message.proto
To re-compile `message.proto` be sure that:

* You have protoc correctly compiled and installed. The easiest way would be to `brew install protobuf`, but you can also download from the [release page](https://github.com/protocolbuffers/protobuf/releases/tag/v3.13.0).
    * Check that the installation was successful with `protoc --version`.

* Set your `$GOPATH` to `/home/user/go` to avoid funny issues with protoc dependencies.

*  Install protoc-gen-go
```
go install google.golang.org/protobuf/cmd/protoc-gen-go
```

* Install [`gogo` dependencies](https://github.com/gogo/protobuf) used by our protobuf
```
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/gogoproto
go get github.com/gogo/protobuf/protoc-gen-gofaster
```

* You are ready to compile the .proto
```
make
```

### Common issues
* If you don't set your `$GOPATH` correctly:
```
github.com/gogo/protobuf/gogoproto/gogo.proto:32:1: Import "google/protobuf/descriptor.proto" was not found or had errors.
```

* If you haven't installed the `gogo` dependencies correctly:
```
protoc --proto_path=/home/adlrocha/go/src:. --gogofaster_out=. message.proto
protoc-gen-gogofaster: program not found or is not executable
Please specify a program using absolute path or make sure the program is available in your PATH system variable
--gogofaster_out: protoc-gen-gogofaster: Plugin failed with status code 1.
make: *** [Makefile:7: message.pb.go] Error 1
```

* Let us know in the issues if there are any additional common mistakes you want us to document here.