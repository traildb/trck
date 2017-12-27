# Protobuf Output

Passing a properly conforming .proto file to trck with `--proto` will enable protobuf output on the resulting matcher. (The matcher will still default to JSON output)

Passing `--output-format proto` when running the matcher will make the matcher output protobuf.

### Output Format

Each result is preceded by the length of the encoded message, even when there is only one result. See `proto-example/test.py` for an example of how to parse this in Python. 

### Proto File

Create a Protobuf message `Result` in package `trck` and create a field for each parameter and output variable according to the following rules:
* Encode parameter `x` as a string and name it `scalar_x`
* Encode counter `x` as any integer type and name it `counter_x`
* Encode set `x` as `repeated trck.SetTuple`, import `SetTuple.proto`, and name it `set_x`
* Encode multiset `x` as `repeated trck.MultisetTuple`, import `MultisetTuple.proto` and name it `multiset_x`
* Encode hll `x` as `trck.Hll`, import `Hll.proto` and name it `hll_x`

trck will ignore extra fields in the `Result` message.

### Extra Data Types
trck defines a few protobuf data types to help you encode your output. They are
* SetTuple
* MultisetTuple
* Hll

These are included into the project automatically, the user must only import the ones they need into their `Result` definition.

### Sets and Multisets

All sets must be encoded as `repeated trck.SetTuple`. The `SetTuple` message is a data type included automatically by trck and has the following structure:
```
message SetTuple {
  repeated string values = 1;
}
```
The multiset is very similar except that it uses a tuple type which comes with an associated `count` value:
```
message MultisetTuple {
  repeated string values = 1;
  uint64 count = 2;
}
```

Even when yielding a single value to a set, a tuple is necessary. In this case, the `values` array will be length 1. 


### HLLs
The HLL data type is defined as follows:
```
message Hll {
	uint32 precision = 1;
	bool empty = 2;
	bytes bins = 3;
}
```
`precision` is the precision of the HLL datastructure.

`empty` is a boolean which is False if and only if the HLL is completely empty

`bins` is a run length encoded representation of the bins. There are `2^precision` bins.
