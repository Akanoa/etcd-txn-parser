use etcd_txn_parser::compare::{Compare, ModRevision, OpType};
use etcd_txn_parser::operation::{Operation, PutData};
use etcd_txn_parser::TxnData;
use noa_parser::visitor::Visitor;

#[test]
fn test_transaction() {
    let transaction = include_bytes!("fixtures/simple.txt");
    let mut scanner = noa_parser::scanner::Scanner::new(transaction);
    let result = TxnData::accept(&mut scanner).expect("Failed to parse");
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
    let mut scanner = noa_parser::scanner::Scanner::new(transaction);
    let result = TxnData::accept(&mut scanner).expect("Failed to parse");
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
    let mut scanner = noa_parser::scanner::Scanner::new(transaction);
    let result = TxnData::accept(&mut scanner).expect("Failed to parse");
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
    let mut scanner = noa_parser::scanner::Scanner::new(transaction);
    let result = TxnData::accept(&mut scanner).expect("Failed to parse");
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
