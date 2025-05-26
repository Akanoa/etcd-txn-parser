use crate::compare::Compare;
use crate::operation::Operation;
use noa_parser::bytes::matchers::match_pattern;
use noa_parser::bytes::primitives::whitespace::OptionalWhitespaces;
use noa_parser::bytes::token::Token;
use noa_parser::errors::{ParseError, ParseResult};
use noa_parser::matcher::{Match, MatchSize};
use noa_parser::peek::{peek, Until, UntilEnd};
use noa_parser::recognizer::recognize;
use noa_parser::scanner::Scanner;
use noa_parser::separated_list::SeparatedList;
use noa_parser::visitor::Visitor;

pub mod compare;
pub mod operation;

/// A transactional data structure.
#[derive(Debug, PartialEq)]
pub struct TxnData<'a> {
    /// A list of operations to compare against the current state.
    pub compares: Vec<Compare<'a>>,
    /// A list of operations to apply if the compare operations pass.
    pub success: Vec<Operation<'a>>,
    /// A list of operations to apply if the compare operations fail.
    pub failure: Vec<Operation<'a>>,
}

struct LineFeed;

impl<'a> Visitor<'a, u8> for LineFeed {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        recognize(Token::Ln, scanner)?;
        Ok(LineFeed)
    }
}

#[derive(Clone)]
struct SectionEnd;

impl Match<u8> for SectionEnd {
    fn matcher(&self, data: &[u8]) -> (bool, usize) {
        match_pattern(b"\n\n", data)
    }
}

impl MatchSize for SectionEnd {
    fn size(&self) -> usize {
        2
    }
}

impl<'a> Visitor<'a, u8> for TxnData<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;

        // Read the compare section
        let section_compare =
            peek(Until::new(SectionEnd), scanner)?.ok_or(ParseError::UnexpectedToken)?;
        let mut compares = vec![];
        if !section_compare.data.is_empty() {
            let mut section_compare_scanner = Scanner::new(section_compare.data);
            compares =
                SeparatedList::<u8, Compare, LineFeed>::accept(&mut section_compare_scanner)?
                    .into_iter()
                    .collect();
            scanner.bump_by(section_compare.end_slice);
        }
        LineFeed::accept(scanner)?;
        LineFeed::accept(scanner)?;

        // Read the success section
        let section_success =
            peek(Until::new(SectionEnd), scanner)?.ok_or(ParseError::UnexpectedToken)?;
        let mut success = vec![];
        if !section_success.data.is_empty() {
            let mut section_success_scanner = Scanner::new(section_success.data);
            success =
                SeparatedList::<u8, Operation, LineFeed>::accept(&mut section_success_scanner)?
                    .into_iter()
                    .collect();
            scanner.bump_by(section_success.end_slice);
        }
        LineFeed::accept(scanner)?;
        LineFeed::accept(scanner)?;

        // Read the failure section
        let section_failure =
            peek(UntilEnd::default(), scanner)?.ok_or(ParseError::UnexpectedToken)?;
        let mut failure = vec![];
        if !section_failure.data.is_empty() {
            let mut section_failure_scanner = Scanner::new(section_failure.data);
            failure =
                SeparatedList::<u8, Operation, LineFeed>::accept(&mut section_failure_scanner)?
                    .into_iter()
                    .collect();
        }

        Ok(TxnData {
            compares,
            success,
            failure,
        })
    }
}
