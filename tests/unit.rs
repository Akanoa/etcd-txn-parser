use etcd_txn_parser::compare::{Compare, ModRevision, OpType, Value};
use etcd_txn_parser::operation::{DeleteData, GetData, Operation, PutData};
use etcd_txn_parser::{parse, TxnData};

#[test]
fn test_transaction() {
    let transaction = include_bytes!("fixtures/simple.txt");
    let result = parse(transaction).expect("Failed to parse");
    assert_eq!(
        result,
        TxnData {
            compares: vec![Compare::ModRevision(ModRevision {
                key: b"key1",
                value: 0,
                op: OpType::GreaterThan
            })],
            success: vec![Operation::Put(PutData {
                key: b"key1",
                value: b"overwrote-key1"
            })],
            failure: vec![
                Operation::Put(PutData {
                    key: b"key1",
                    value: b"created-key1"
                }),
                Operation::Put(PutData {
                    key: b"key2",
                    value: b"some extra key"
                })
            ]
        }
    )
}

#[test]
fn test_transaction_no_compare() {
    let transaction = include_bytes!("fixtures/no_compare.txt");
    let result = parse(transaction).expect("Failed to parse");
    assert_eq!(
        result,
        TxnData {
            compares: vec![],
            success: vec![Operation::Put(PutData {
                key: b"key1",
                value: b"overwrote-key1"
            })],
            failure: vec![
                Operation::Put(PutData {
                    key: b"key1",
                    value: b"created-key1"
                }),
                Operation::Put(PutData {
                    key: b"key2",
                    value: b"some extra key"
                })
            ]
        }
    )
}

#[test]
fn test_transaction_no_success() {
    let transaction = include_bytes!("fixtures/no_success.txt");
    let result = parse(transaction).expect("Failed to parse");
    assert_eq!(
        result,
        TxnData {
            compares: vec![Compare::ModRevision(ModRevision {
                key: b"key1",
                value: 0,
                op: OpType::GreaterThan
            })],
            success: vec![],
            failure: vec![
                Operation::Put(PutData {
                    key: b"key1",
                    value: b"created-key1"
                }),
                Operation::Put(PutData {
                    key: b"key2",
                    value: b"some extra key"
                })
            ]
        }
    )
}

#[test]
fn test_transaction_no_failure() {
    let transaction = include_bytes!("fixtures/no_failure.txt");
    let result = parse(transaction).expect("Failed to parse");
    assert_eq!(
        result,
        TxnData {
            compares: vec![Compare::ModRevision(ModRevision {
                key: b"key1",
                value: 0,
                op: OpType::GreaterThan
            })],
            success: vec![Operation::Put(PutData {
                key: b"key1",
                value: b"overwrote-key1"
            })],
            failure: vec![]
        }
    )
}

#[test]
fn test_transaction_val_key() {
    let transaction = include_bytes!("fixtures/val_key.txt");
    let result = parse(transaction).expect("Failed to parse");
    assert_eq!(
        result,
        TxnData {
            compares: vec![Compare::Value(Value {
                key: b"key",
                value: b"toto",
                op: OpType::Equal
            })],
            success: vec![],
            failure: vec![Operation::Put(PutData {
                key: b"key",
                value: b"toto"
            })]
        }
    )
}

#[test]
fn test_transaction_just_success() {
    let transaction = include_bytes!("fixtures/just_success.txt");
    let result = parse(transaction).expect("Failed to parse");
    assert_eq!(
        result,
        TxnData {
            compares: vec![],
            success: vec![
                Operation::Get(GetData { key: b"key1" }),
                Operation::Get(GetData { key: b"key2" }),
                Operation::Get(GetData { key: b"key3" }),
                Operation::Delete(DeleteData { key: b"key4" })
            ],
            failure: vec![]
        }
    )
}
