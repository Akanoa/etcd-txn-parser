# Etcd transaction parser

Uses the [noa-parser](https://github.com/akanoa/noa-parser) library to parse
[etcd transaction files](https://github.com/etcd-io/etcd/blob/main/etcdctl/README.md#txn-options).

```bnf
<Txn> ::= <CMP>* "\n" <THEN> "\n" <ELSE> "\n"
<CMP> ::= (<CMPCREATE>|<CMPMOD>|<CMPVAL>|<CMPVER>|<CMPLEASE>) "\n"
<CMPOP> ::= "<" | "=" | ">"
<CMPCREATE> := ("c"|"create")"("<KEY>")" <CMPOP> <REVISION>
<CMPMOD> ::= ("m"|"mod")"("<KEY>")" <CMPOP> <REVISION>
<CMPVAL> ::= ("val"|"value")"("<KEY>")" <CMPOP> <VALUE>
<CMPVER> ::= ("ver"|"version")"("<KEY>")" <CMPOP> <VERSION>
<CMPLEASE> ::= "lease("<KEY>")" <CMPOP> <LEASE>
<THEN> ::= <OP>*
<ELSE> ::= <OP>*
<OP> ::= ((see put, get, del etcdctl command syntax)) "\n"
<KEY> ::= (%q formatted string)
<VALUE> ::= (%q formatted string)
<REVISION> ::= "\""[0-9]+"\""
<VERSION> ::= "\""[0-9]+"\""
<LEASE> ::= "\""[0-9]+\""
```

## Usage

```rust
use etcd_txn_parser::parse;

fn main() {
    let txn = r#"mod("key1") > 0

put key1 "overwrote-key1"

put "key1" "created-key1"
put key2 "some extra key""#;

    let txn = parse(txn.as_bytes());

    println!("{txn:#?}");
}
```