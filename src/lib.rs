#![doc = include_str!("../Readme.md")]
use crate::compare::Compare;
use crate::operation::Operation;
use elyze::bytes::matchers::match_pattern;
use elyze::bytes::primitives::whitespace::OptionalWhitespaces;
use elyze::bytes::token::Token;
use elyze::errors::{ParseError, ParseResult};
use elyze::matcher::Match;
use elyze::peek::{peek, DefaultPeekableImplementation, PeekableImplementation, UntilEnd};
use elyze::recognizer::recognize;
use elyze::scanner::Scanner;
use elyze::separated_list::SeparatedList;
use elyze::visitor::Visitor;

pub mod compare;
pub mod operation;

/// Parse a transactional data structure from a byte slice.
///
/// # Errors
///
/// If the parser encounters an unexpected token, a `ParseError` is returned.
///
/// # Examples
///
///
pub fn parse(data: &[u8]) -> ParseResult<TxnData> {
    TxnData::accept(&mut Scanner::new(data))
}

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

#[derive(Clone, Default)]
struct SectionEnd;

impl Match<u8> for SectionEnd {
    fn is_matching(&self, data: &[u8]) -> (bool, usize) {
        match_pattern(b"\n\n", data)
    }

    fn size(&self) -> usize {
        2
    }
}

impl PeekableImplementation for SectionEnd {
    type Type = DefaultPeekableImplementation;
}

impl<'a> Visitor<'a, u8> for TxnData<'a> {
    fn accept(scanner: &mut Scanner<'a, u8>) -> ParseResult<Self> {
        OptionalWhitespaces::accept(scanner)?;

        // Read the compare section
        let section_compare = peek(SectionEnd, scanner)?.ok_or(ParseError::UnexpectedToken)?;
        let mut section_compare_scanner = Scanner::new(section_compare.peeked_slice());
        let compares =
            SeparatedList::<u8, Compare, LineFeed>::accept(&mut section_compare_scanner)?.data;
        scanner.bump_by(section_compare.end_slice);

        // Read the success section
        let section_success = peek(SectionEnd, scanner)?.ok_or(ParseError::UnexpectedToken)?;

        let mut section_success_scanner = Scanner::new(section_success.peeked_slice());
        let success =
            SeparatedList::<u8, Operation, LineFeed>::accept(&mut section_success_scanner)?.data;
        scanner.bump_by(section_success.end_slice);

        // Read the failure section
        let section_failure =
            peek(UntilEnd::default(), scanner)?.ok_or(ParseError::UnexpectedToken)?;

        let mut section_failure_scanner = Scanner::new(section_failure.peeked_slice());
        let failure =
            SeparatedList::<u8, Operation, LineFeed>::accept(&mut section_failure_scanner)?.data;

        scanner.bump_by(section_failure.end_slice);

        Ok(TxnData {
            compares,
            success,
            failure,
        })
    }
}
