use etcd_txn_parser::parse;

fn main() {
    let txn = r#"mod("key1") > 0

put key1 "overwrote-key1"

put "key1" "created-key1"
put key2 "some extra key""#;

    let txn = parse(txn.as_bytes());

    println!("{txn:#?}");
}
