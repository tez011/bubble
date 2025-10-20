use crate::common::{Binding, CoreForm, Environment as CommonEnvironment};
use crate::io::InputPort;
use std::borrow::Cow;
use std::rc::Rc;

pub type Scope = usize;
pub type ScopeSet = std::collections::BTreeSet<Scope>;
type MatchEnv<'a> = std::collections::HashMap<&'a str, MatchValue>;
type MatchSlice<'a, 'b> = std::collections::HashMap<&'a str, &'b MatchValue>;

#[derive(Debug, Clone)]
pub struct SyntaxObject {
    e: Datum,
    children: Vec<Rc<SyntaxObject>>,
    scopes: ScopeSet,
    source_location: (usize, usize),
}

#[derive(Debug, Clone)]
pub enum Expression {
    CorePrimitive(&'static str),
    Variable(usize),
    Literal(Literal),
    ProcedureCall { operator: Box<Expression>, operands: Vec<Expression> },
    Lambda { formals: (Vec<Expression>, Option<Box<Expression>>), body: Vec<Expression> },
    Conditional { test: Box<Expression>, consequent: Box<Expression>, alternate: Option<Box<Expression>> },
    Assignment { ids: (Vec<Expression>, Option<Box<Expression>>), value: Box<Expression> },
    Definition { ids: (Vec<Expression>, Option<Box<Expression>>), value: Box<Expression> },
    Block { body: Vec<Expression> },
}
impl From<Literal> for Expression { fn from(x: Literal) -> Self { Self::Literal(x) } }
impl From<LiteralC> for Expression { fn from(x: LiteralC) -> Self { Self::Literal(Literal::Copy(x)) } }
impl From<LiteralD> for Expression { fn from(x: LiteralD) -> Self { Self::Literal(Literal::NoCopy(x)) } }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Copy(LiteralC),
    NoCopy(LiteralD),
}
impl From<LiteralC> for Literal { fn from(x: LiteralC) -> Self { Self::Copy(x) } }
impl From<LiteralD> for Literal { fn from(x: LiteralD) -> Self { Self::NoCopy(x) } }
impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Copy(lc) => write!(f, "{}", lc),
            Literal::NoCopy(ld) => write!(f, "{}", ld),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum LiteralC {
    #[default] Nil,
    Boolean(bool),
    Integer(i64),
    Rational(i64, i64),
    Float(f32),
    Character(char),
}
impl PartialEq for LiteralC {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean(l0), Self::Boolean(r0)) => l0 == r0,
            (Self::Integer(l0), Self::Integer(r0)) => l0 == r0,
            (Self::Rational(l0, l1), Self::Rational(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::Character(l0), Self::Character(r0)) => l0 == r0,
            _ => false,
        }
    }
}
impl Eq for LiteralC {
}
impl std::fmt::Display for LiteralC {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralC::Nil => write!(f, "()"),
            LiteralC::Boolean(true) => write!(f, "#t"),
            LiteralC::Boolean(false) => write!(f, "#f"),
            LiteralC::Integer(i) => write!(f, "{}", i),
            LiteralC::Rational(n, d) => write!(f, "{}/{}", n, d),
            LiteralC::Float(x) => write!(f, "{}", x),
            LiteralC::Character(c) => write!(f, "#\\{}", c),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LiteralD {
    #[default] Nil,
    Static(&'static str),
    String(String),
    Bytes(Vec<u8>),
    Symbol(String),
    List(Vec<Literal>, Option<Box<Literal>>),
    Vector(Vec<Literal>),
}
impl std::fmt::Display for LiteralD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralD::Nil => write!(f, "()"),
            LiteralD::Static(s) => write!(f, "{}", s),
            LiteralD::String(s) => write!(f, "{:?}", s),
            LiteralD::Bytes(items) => {
                write!(f, "#u8(")?;
                for (i, x) in items.iter().enumerate() {
                    write!(f, "{}{}", x, if i + 1 == items.len() { ')' } else { ' ' })?;
                }
                Ok(())
            }
            LiteralD::Symbol(s) => write!(f, "{}", s),
            LiteralD::List(literals, tail) => {
                write!(f, "(")?;
                for (i, l) in literals.iter().enumerate() {
                    if i == 0 { write!(f, "{}", l)?; }
                    else { write!(f, " {}", l)?; }
                }
                match tail {
                    None => write!(f, ")"),
                    Some(tail) => write!(f, " . {})", tail),
                }
            }
            LiteralD::Vector(literals) => {
                write!(f, "(")?;
                for (i, l) in literals.iter().enumerate() {
                    if i == 0 { write!(f, "{}", l)?; }
                    else { write!(f, " {}", l)?; }
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Rules(pub Vec<(Pattern, Template)>);

#[derive(Debug, Clone)]
pub enum Pattern {
    Underscore,
    Literal(Cow<'static, str>),
    Variable(Cow<'static, str>),
    List(Vec<Pattern>),
    ImproperList(Vec<Pattern>, Box<Pattern>),
    Vector(Vec<Pattern>),
    Ellipsis(Box<Pattern>),
    Constant(Literal),
}

#[derive(Debug, Clone)]
pub enum Template {
    Constant(Rc<SyntaxObject>),
    Identifier(Cow<'static, str>),
    Ellipsis(Box<Template>),
    List(Vec<Template>),
    ImproperList(Vec<Template>, Box<Template>),
    Vector(Vec<Template>),
}

#[derive(Debug)]
struct Environment<'p> {
    parent_env: &'p mut CommonEnvironment,
    bindings: std::collections::HashMap<std::borrow::Cow<'static, str>, std::collections::BTreeMap<ScopeSet, Binding>>,
    scope_counter: std::cell::Cell<usize>,
}
impl<'p> From<&'p mut CommonEnvironment> for Environment<'p> {
    fn from(parent_env: &'p mut CommonEnvironment) -> Self {
        let bindings = parent_env.toplevels.iter()
            .map(|(name, binding)| (name.clone(), std::iter::once((ScopeSet::default(), *binding)).collect()))
            .collect();
        Self {
            parent_env,
            bindings,
            scope_counter: std::cell::Cell::new(1),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    NoToken,
    UnexpectedToken,
    IoError(std::io::Error),
    InvalidNumber(&'static str),
    InvalidCharacter,

    BadSyntax(Rc<SyntaxObject>),
    BadCoreFormSyntax(CoreForm, Rc<SyntaxObject>),
    AmbiguousBinding(Cow<'static, str>),
    NoMatchingPattern(Rc<SyntaxObject>),
    UnboundIdentifier(Rc<SyntaxObject>),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoToken => write!(f, "No token found"),
            Error::UnexpectedToken => write!(f, "Unexpected token"),
            Error::IoError(e) => write!(f, "I/O error: {}", e),
            Error::InvalidNumber(msg) => write!(f, "Invalid number: {}", msg),
            Error::InvalidCharacter => write!(f, "Invalid character"),
            Error::BadSyntax(stx) => write!(f, "Bad syntax at {}:{}: {}", stx.source_location.0, stx.source_location.1, stx),
            Error::BadCoreFormSyntax(c, stx) => write!(f, "Bad core-form syntax for {} at {}:{}: {}", c.as_str(), stx.source_location.0, stx.source_location.1, stx),
            Error::AmbiguousBinding(name) => write!(f, "Bad syntax due to ambiguous binding: {}", name),
            Error::NoMatchingPattern(stx) => write!(f, "No macro expansion for pattern: {}", stx),
            Error::UnboundIdentifier(stx) => match &stx.e {
                Datum::Symbol(name) => write!(f, "Unbound identifier: {} at {}:{}", name, stx.source_location.0, stx.source_location.1),
                _ => unreachable!(),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Identifier(String),
    Boolean(bool),
    Number(String, TokenTag),
    Character(String),
    String(String),
    Open,
    Close,
    Vector,
    Bytes,
    Quote,
    Backquote,
    Unquote,
    UnquoteSplicing,
    Dot,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenTag {
    Integer,
    Rational,
    Decimal,
    Infinity,
    NaN,
}

#[derive(Debug, Clone, PartialEq)]
enum Datum {
    Boolean(bool),
    Integer(i64),
    Rational(i64, i64),
    Float(f32),
    Character(char),
    String(String),
    Symbol(Cow<'static, str>),
    Bytes(Vec<u8>),
    List,
    ImproperList,
    Vector,
    Quote,
    Quasiquote,
    Unquote,
    UnquoteSplicing,
}

#[derive(Debug, Clone)]
pub enum MatchValue {
    One(Rc<SyntaxObject>),
    Many(Vec<MatchValue>),
}

#[derive(Debug, Clone)]
enum QQExpansion {
    Literal(Literal),
    Unquote(Expression),
    UnquoteSplicing(Expression),
}

static SYNTAX_DEFINITION_PATTERN: std::sync::LazyLock<Pattern> = std::sync::LazyLock::new(|| Pattern::List(vec![
    Pattern::Literal(Cow::Borrowed("define-syntax")),
    Pattern::Variable(Cow::Borrowed("name")),
    Pattern::List(vec![
        Pattern::Literal(Cow::Borrowed("syntax-rules")),
        Pattern::Variable(Cow::Borrowed("literals")),
        Pattern::Ellipsis(Box::new(Pattern::List(vec![
            Pattern::Variable(Cow::Borrowed("pattern")),
            Pattern::Variable(Cow::Borrowed("template")),
        ]))),
    ]),
]));
static VALUE_DEFINITION_PATTERN: std::sync::LazyLock<Pattern> = std::sync::LazyLock::new(|| Pattern::List(vec![
    Pattern::Literal(Cow::Borrowed("define-values")),
    Pattern::List(vec![Pattern::Ellipsis(Box::new(Pattern::Variable(Cow::Borrowed("formals"))))]),
    Pattern::Variable(Cow::Borrowed("expression")),
]));
static SYNTAX_BINDING_PATTERN: std::sync::LazyLock<Pattern> = std::sync::LazyLock::new(|| Pattern::ImproperList(vec![
    Pattern::Literal(Cow::Borrowed("letrec-syntax")),
    Pattern::List(vec![
        Pattern::Ellipsis(Box::new(Pattern::List(vec![
            Pattern::Variable(Cow::Borrowed("name")),
            Pattern::List(vec![
                Pattern::Literal(Cow::Borrowed("syntax-rules")),
                Pattern::Variable(Cow::Borrowed("literals")),
                Pattern::Ellipsis(Box::new(Pattern::List(vec![
                    Pattern::Variable(Cow::Borrowed("pattern")),
                    Pattern::Variable(Cow::Borrowed("template")),
                ]))),
            ]),
        ]))),
    ]),
], Box::new(Pattern::Variable(Cow::Borrowed("body")))));

impl SyntaxObject {
    fn simple(e: Datum, source_location: (usize, usize)) -> Self {
        Self::compound(e, Vec::new(), source_location)
    }
    fn compound(e: Datum, children: Vec<SyntaxObject>, source_location: (usize, usize)) -> Self {
        SyntaxObject {
            e,
            children: children.into_iter().map(Rc::new).collect(),
            scopes: Default::default(),
            source_location
        }
    }

    fn add_scope(&mut self, scope: Scope) {
        match self.e {
            Datum::Symbol(_) | Datum::Quote => {
                self.scopes.insert(scope);
            },
            Datum::List | Datum::ImproperList | Datum::Quasiquote | Datum::Unquote | Datum::UnquoteSplicing => {
                self.scopes.insert(scope);
                self.children.iter_mut()
                    .filter(|stx| matches!(stx.e, Datum::Symbol(_) | Datum::List | Datum::ImproperList | Datum::Quasiquote | Datum::Unquote | Datum::UnquoteSplicing))
                    .for_each(|stx| Rc::make_mut(stx).add_scope(scope));
            },
            _ => (),
        }
    }

    fn is_literal(&self, e: &Literal) -> bool {
        match (&self.e, e) {
            (Datum::Boolean(e0), Literal::Copy(LiteralC::Boolean(e1))) => e0 == e1,
            (Datum::Integer(e0), Literal::Copy(LiteralC::Integer(e1))) => e0 == e1,
            (Datum::Rational(e0, e2), Literal::Copy(LiteralC::Rational(e1, e3))) => e0 == e1 && e2 == e3,
            (Datum::Float(e0), Literal::Copy(LiteralC::Float(e1))) => e0 == e1,
            (Datum::Character(e0), Literal::Copy(LiteralC::Character(e1))) => e0 == e1,
            (Datum::String(e0), Literal::NoCopy(LiteralD::String(e1))) => e0 == e1,
            (Datum::Symbol(e0), Literal::NoCopy(LiteralD::Symbol(e1))) => e0 == e1,
            (Datum::Symbol(e0), Literal::NoCopy(LiteralD::Static(e1))) => e0 == e1,
            (Datum::Bytes(e0), Literal::NoCopy(LiteralD::Bytes(e1))) => e0 == e1,
            (Datum::List | Datum::ImproperList | Datum::Vector | Datum::Quote | Datum::Quasiquote | Datum::Unquote | Datum::UnquoteSplicing, _) => &Literal::from(self) == e,
            _ => false,
        }
    }
}
impl std::fmt::Display for SyntaxObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.e {
            Datum::Boolean(true) => write!(f, "#t"),
            Datum::Boolean(false) => write!(f, "#f"),
            Datum::Integer(i) => write!(f, "{}", i),
            Datum::Rational(n, d) => write!(f, "{}/{}", n, d),
            Datum::Float(fl) => write!(f, "{}", fl),
            Datum::Character(c) => write!(f, "#\\{}", c),
            Datum::String(s) => write!(f, "\"{}\"", s),
            Datum::Bytes(b) => write!(f, "{:?}", b),
            Datum::Symbol(s) => write!(f, "{}", s),
            Datum::Quote => write!(f, "'{}", self.children[0]),
            Datum::Quasiquote => write!(f, "`{}", self.children[0]),
            Datum::Unquote => write!(f, ",{}", self.children[0]),
            Datum::UnquoteSplicing => write!(f, ",@{}", self.children[0]),
            Datum::List => {
                write!(f, "(")?;
                for (i, child) in self.children.iter().enumerate() {
                    write!(f, "{}{}", child, if i + 1 == self.children.len() { ')' } else { ' ' })?;
                }
                Ok(())
            },
            Datum::ImproperList => {
                write!(f, "(")?;
                for (i, child) in self.children.iter().enumerate() {
                    match self.children.len() - i {
                        1 => write!(f, ". {})", child),
                        _ => write!(f, "{} ", child),
                    }?;
                }
                Ok(())
            },
            Datum::Vector => {
                write!(f, "#(")?;
                for (i, child) in self.children.iter().enumerate() {
                    write!(f, "{}{}", child, if i + 1 == self.children.len() { ')' } else { ' ' })?;
                }
                Ok(())
            },
        }
    }
}

impl From<&SyntaxObject> for Literal {
    fn from(stx: &SyntaxObject) -> Self {
        match &stx.e {
            Datum::Boolean(b) => LiteralC::Boolean(*b).into(),
            Datum::Integer(i) => LiteralC::Integer(*i).into(),
            Datum::Rational(n, d) => LiteralC::Rational(*n, *d).into(),
            Datum::Float(f) => LiteralC::Float(*f).into(),
            Datum::Character(c) => LiteralC::Character(*c).into(),
            Datum::String(s) => LiteralD::String(s.clone()).into(),
            Datum::Bytes(items) => LiteralD::Bytes(items.clone()).into(),
            Datum::Symbol(Cow::Borrowed(s)) => LiteralD::Static(s).into(),
            Datum::Symbol(Cow::Owned(s)) => LiteralD::Symbol(s.clone()).into(),
            Datum::List => {
                if stx.children.len() == 0 {
                    return LiteralC::Nil.into();
                }
                if let Datum::Symbol(name) = &stx.children.first().unwrap().e {
                    if stx.children.len() > 1 {
                        match name.as_ref() {
                            "quote" => return LiteralD::List(vec![LiteralD::Static("quote").into(), stx.children[1].as_ref().into()], None).into(),
                            "quasiquote" => return LiteralD::List(vec![LiteralD::Static("quasiquote").into(), stx.children[1].as_ref().into()], None).into(),
                            "unquote" => return LiteralD::List(vec![LiteralD::Static("unquote").into(), stx.children[1].as_ref().into()], None).into(),
                            "unquote-splicing" => return LiteralD::List(vec![LiteralD::Static("unquote-splicing").into(), stx.children[1].as_ref().into()], None).into(),
                            _ => (),
                        }
                    }
                }
                LiteralD::List(stx.children.iter().map(|stx| stx.as_ref().into()).collect(), None).into()
            },
            Datum::ImproperList => {
                let mut children = stx.children.iter().map(|stx| stx.as_ref().into()).collect::<Vec<_>>();
                let tail = children.pop().map(Box::new);
                LiteralD::List(children, tail).into()
            },
            Datum::Vector => LiteralD::Vector(stx.children.iter().map(|stx| stx.as_ref().into()).collect()).into(),
            Datum::Quote => LiteralD::List(vec![LiteralD::Static("quote").into(), stx.children[0].as_ref().into()], None).into(),
            Datum::Quasiquote => LiteralD::List(vec![LiteralD::Static("quasiquote").into(), stx.children[0].as_ref().into()], None).into(),
            Datum::Unquote => LiteralD::List(vec![LiteralD::Static("unquote").into(), stx.children[0].as_ref().into()], None).into(),
            Datum::UnquoteSplicing => LiteralD::List(vec![LiteralD::Static("unquote-splicing").into(), stx.children[0].as_ref().into()], None).into(),
        }
    }
}

impl<'a> Pattern {
    pub fn try_match(&'a self, input: &Rc<SyntaxObject>) -> Option<MatchEnv<'a>> {
        match self {
            Pattern::Underscore => Some(Default::default()),
            Pattern::Literal(s) => {
                if let Datum::Symbol(sym) = &input.e {
                    if sym == s {
                        return Some(Default::default());
                    }
                }
                None
            }
            Pattern::Variable(v) => Some([(v.as_ref(), MatchValue::One(input.clone()))].into_iter().collect()),
            Pattern::List(patterns) if matches!(input.e, Datum::List) => Self::try_match_sequence(patterns, &input.children, None),
            Pattern::ImproperList(patterns, tailcdr) => {
                if matches!(input.e, Datum::List) {
                    return Self::try_match_sequence(patterns, &input.children, Some(tailcdr));
                } else if matches!(input.e, Datum::ImproperList) {
                    if let Some(mut env) = Self::try_match_sequence(patterns, &input.children[..input.children.len()-1], None) {
                        if let Some(tenv) = tailcdr.try_match(input.children.last().unwrap()) {
                            env.extend(tenv);
                            return Some(env);
                        }
                    }
                }
                None
            }
            Pattern::Vector(patterns) if matches!(input.e, Datum::Vector) => Self::try_match_sequence(patterns, &input.children, None),
            Pattern::Ellipsis(_) => None, // not allowed here
            Pattern::Constant(c) if input.is_literal(c) => Some(Default::default()),
            _ => None,
        }
    }

    fn try_match_sequence(patterns: &'a [Pattern], inputs: &[Rc<SyntaxObject>], tailcdr: Option<&'a Pattern>) -> Option<MatchEnv<'a>> {
        let mut pit = patterns.iter();
        let mut si = 0;
        let mut env = MatchEnv::new();
        while let Some(pattern) = pit.next() {
            match pattern {
                Pattern::Ellipsis(inner) => if si < inputs.len() {
                    while let Some(e) = inner.try_match(&inputs[si]) {
                        for (k, v) in e {
                            match env.get_mut(k) {
                                Some(MatchValue::Many(vs)) => vs.push(v),
                                Some(MatchValue::One(_)) => panic!(),
                                None => { env.insert(k, MatchValue::Many(vec![v])); },
                            }
                        }

                        si += 1;
                        if si >= inputs.len() {
                            break;
                        }
                    }
                },
                _ => match inputs.get(si) {
                    Some(stx) => match pattern.try_match(stx) {
                        Some(e) => {
                            env.extend(e);
                            si += 1;
                        },
                        None => return None,
                    },
                    None => return None,
                }
            }
        }

        if si < inputs.len() {
            // There are remaining inputs that need to be matched against the tail pattern. They form a list.
            match tailcdr {
                None => None,
                Some(Pattern::Underscore) => Some(env),
                Some(Pattern::Literal(_)) | Some(Pattern::Constant(_)) => None,
                Some(Pattern::Variable(v)) => {
                    env.insert(v, MatchValue::Many(inputs[si..].iter().map(|s| MatchValue::One(s.clone())).collect()));
                    Some(env)
                },
                Some(Pattern::List(sp)) | Some(Pattern::Vector(sp)) => {
                    env.extend(Self::try_match_sequence(sp, &inputs[si..], None)?);
                    Some(env)
                },
                Some(Pattern::ImproperList(sp, tailcdr)) => {
                    env.extend(Self::try_match_sequence(sp, &inputs[si..], Some(tailcdr))?);
                    Some(env)
                },
                Some(Pattern::Ellipsis(_)) => None, // not allowed here
            }
        } else {
            Some(env)
        }
    }

    fn from_sequence(children: &[Rc<SyntaxObject>], literals: &std::collections::HashSet<&str>) -> Result<Vec<Self>, ()> {
        let mut patterns = Vec::new();
        let mut it = children.iter().peekable();
        while let Some(child) = it.next() {
            if let Ok(subpat) = Pattern::from(child, literals) {
                if let Some(lookahead) = it.peek() {
                    if let Datum::Symbol(lookahead) = &lookahead.e {
                        if lookahead == "..." {
                            it.next();
                            patterns.push(Pattern::Ellipsis(Box::new(subpat)));
                        } else {
                            patterns.push(subpat);
                        }
                    } else {
                        patterns.push(subpat);
                    }
                } else {
                    patterns.push(subpat);
                }
            } else {
                return Err(());
            }
        }

        Ok(patterns)
    }

    fn from(stx: &Rc<SyntaxObject>, literals: &std::collections::HashSet<&str>) -> Result<Self, ()> {
        match &stx.e {
            Datum::Symbol(s) if s == "_" => Ok(Pattern::Underscore),
            Datum::Symbol(s) => if literals.contains(s.as_ref()) { Ok(Pattern::Literal(s.clone())) } else { Ok(Pattern::Variable(s.clone())) },
            Datum::Boolean(_) | Datum::Integer(_) | Datum::Rational(..) | Datum::Character(_) | Datum::Float(_) | Datum::String(_) | Datum::Bytes(_) => Ok(Pattern::Constant(stx.as_ref().into())),
            Datum::List => Ok(Pattern::List(Self::from_sequence(&stx.children, literals)?)),
            Datum::Vector => Ok(Pattern::Vector(Self::from_sequence(&stx.children, literals)?)),
            Datum::ImproperList => Ok(Pattern::ImproperList(
                Self::from_sequence(&stx.children[..stx.children.len()-1], literals)?,
                Box::new(Self::from(stx.children.last().unwrap(), literals)?),
            )),
            Datum::Quote | Datum::Quasiquote | Datum::Unquote | Datum::UnquoteSplicing => Err(()),
        }
    }

    fn literals(&'a self) -> std::collections::HashSet<&'a str> {
        let mut literals = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(self);
        while let Some(pattern) = queue.pop_front() {
            match pattern {
                Pattern::Literal(Cow::Borrowed(s)) => { literals.insert(*s); },
                Pattern::Literal(Cow::Owned(s)) => { literals.insert(s.as_str()); },
                Pattern::List(patterns) => queue.extend(patterns),
                Pattern::ImproperList(patterns, tail) => {
                    queue.extend(patterns);
                    queue.push_back(tail);
                }
                Pattern::Vector(patterns) => queue.extend(patterns),
                Pattern::Ellipsis(pattern) => queue.push_back(pattern),
                _ => (),
            }
        }
        literals
    }
}
impl std::fmt::Display for Pattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pattern::Underscore => write!(f, "_"),
            Pattern::Literal(s) => write!(f, "{}", s),
            Pattern::Variable(s) => write!(f, "{}", s),
            Pattern::Ellipsis(pattern) => write!(f, "{} ...", pattern),
            Pattern::Constant(literal) => write!(f, "{}", literal),
            Pattern::List(patterns) => {
                write!(f, "(")?;
                for (i, p) in patterns.iter().enumerate() {
                    write!(f, "{}{}", p, if i + 1 == patterns.len() { ')' } else { ' ' })?;
                }
                Ok(())
            },
            Pattern::ImproperList(patterns, tail) => {
                write!(f, "(")?;
                for p in patterns {
                    write!(f, "{} ", p)?;
                }
                write!(f, ". {})", tail)
            }
            Pattern::Vector(patterns) => {
                write!(f, "#(")?;
                for (i, p) in patterns.iter().enumerate() {
                    write!(f, "{}{}", p, if i + 1 == patterns.len() { ')' } else { ' ' })?;
                }
                Ok(())
            },
        }
    }
}

impl Template {
    fn from_sequence(sequence: &[Rc<SyntaxObject>], literal_ellipsis: bool) -> Result<Vec<Self>, ()> {
        let mut si = 0;
        let mut res = Vec::new();
        while si < sequence.len() {
            let ellipsis_following = match sequence.get(si + 1).map(|s| &s.e) {
                Some(Datum::Symbol(s)) if s == "..." && !literal_ellipsis => true,
                _ => false,
            };
            if ellipsis_following {
                res.push(Template::Ellipsis(Box::new(Template::from(&sequence[si])?)));
                si += 2;
            } else {
                res.push(Template::from(&sequence[si])?);
                si += 1;
            }
        }
        Ok(res)
    }

    fn from_syntax(stx: &Rc<SyntaxObject>, literal_ellipsis: bool) -> Result<Self, ()> {
        match &stx.e {
            Datum::Symbol(s) => Ok(Template::Identifier(s.clone())),
            Datum::List => {
                match stx.children.first().map(|e| &e.e) {
                    Some(Datum::Symbol(head)) if head == "..." => {
                        if let Some(stx) = stx.children.get(1) {
                            Self::from_syntax(stx, true)
                        } else {
                            Err(())
                        }
                    },
                    _ => Ok(Template::List(Self::from_sequence(&stx.children, literal_ellipsis)?)),
                }
            },
            Datum::ImproperList => Ok(Template::ImproperList(
                Self::from_sequence(&stx.children[..stx.children.len()-1], literal_ellipsis)?,
                Box::new(Self::from(stx.children.last().unwrap())?),
            )),
            Datum::Vector => Ok(Template::Vector(Self::from_sequence(&stx.children, literal_ellipsis)?)),
            _ => Ok(Template::Constant(stx.clone())),
        }
    }

    fn from(stx: &Rc<SyntaxObject>) -> Result<Self, ()> {
        Self::from_syntax(stx, false)
    }

    fn count_ellipsis_repetition(&self, env: &MatchSlice) -> Result<usize, ()> {
        let mut reps = None;
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(self);
        loop {
            match queue.pop_front() {
                None => return Ok(reps.unwrap_or(0)),
                Some(Template::Constant(_)) => (),
                Some(Template::Identifier(s)) => match env.get(s.as_ref()) {
                    Some(MatchValue::Many(vs)) => match reps {
                        None => reps = Some(vs.len()),
                        Some(r) if r == vs.len() => (),
                        _ => return Err(()),
                    },
                    Some(MatchValue::One(_)) => unreachable!(),
                    None => (),
                },
                Some(Template::Ellipsis(inner)) => queue.push_back(inner),
                Some(Template::List(templates)) | Some(Template::Vector(templates)) => queue.extend(templates),
                Some(Template::ImproperList(templates, template)) => {
                    queue.extend(templates);
                    queue.push_back(template);
                },
            }
        }
    }
    fn apply_sequence(templates: &[Template], env: &MatchSlice, stx: &Rc<SyntaxObject>, expand_scope: usize) -> Result<Vec<Rc<SyntaxObject>>, Error> {
        let mut result = Vec::new();
        for t in templates {
            match t {
                Template::Ellipsis(inner) => {
                    let n = inner.count_ellipsis_repetition(env).map_err(|_| Error::BadSyntax(stx.clone()))?;
                    for i in 0..n {
                        let env_slice = env.iter()
                            .map(|(k, v)| (*k, match v {
                                MatchValue::One(_) => v,
                                MatchValue::Many(vs) => vs.get(i).unwrap(),
                            }))
                            .collect::<MatchSlice>();
                        result.push(inner.apply(&env_slice, stx, expand_scope)?);
                    }
                },
                _ => result.push(t.apply(env, stx, expand_scope)?),
            }
        }

        Ok(result)
    }
    fn apply(&self, env: &MatchSlice, stx: &Rc<SyntaxObject>, expand_scope: usize) -> Result<Rc<SyntaxObject>, Error> {
        match self {
            Template::Constant(stx) => Ok(stx.clone()),
            Template::Identifier(s) => match env.get(s.as_ref()) {
                Some(MatchValue::One(stx)) => Ok(stx.clone()),
                None => Ok(Rc::new(SyntaxObject {
                    e: Datum::Symbol(s.clone()),
                    children: vec![],
                    scopes: stx.scopes.iter().map(|sc| *sc).chain(std::iter::once(expand_scope)).collect(),
                    source_location: stx.source_location,
                })),
                _ => Err(Error::BadSyntax(stx.clone())),
            }
            Template::Ellipsis(_) => unreachable!(),
            Template::List(templates) => Ok(Rc::new(SyntaxObject {
                e: Datum::List,
                children: Self::apply_sequence(templates, env, stx, expand_scope)?,
                scopes: Default::default(),
                source_location: stx.source_location,
            })),
            Template::ImproperList(templates, template) => Ok(Rc::new(SyntaxObject {
                e: Datum::ImproperList,
                children: Self::apply_sequence(templates, env, stx, expand_scope)?.into_iter()
                    .chain(std::iter::once(template.apply(env, stx, expand_scope)?))
                    .collect(),
                scopes: Default::default(),
                source_location: stx.source_location,
            })),
            Template::Vector(templates) => Ok(Rc::new(SyntaxObject {
                e: Datum::Vector,
                children: Self::apply_sequence(templates, env, stx, expand_scope)?,
                scopes: Default::default(),
                source_location: stx.source_location,
            })),
        }
    }
}
impl std::fmt::Display for Template {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Template::Constant(stx) => write!(f, "{}", stx),
            Template::Identifier(s) => write!(f, "{}", s),
            Template::Ellipsis(template) => write!(f, "{} ...", template),
            Template::List(templates) => {
                write!(f, "(")?;
                for (i, p) in templates.iter().enumerate() {
                    write!(f, "{}{}", p, if i + 1 == templates.len() { ')' } else { ' ' })?;
                }
                Ok(())
            },
            Template::ImproperList(templates, tail) => {
                write!(f, "(")?;
                for p in templates {
                    write!(f, "{} ", p)?;
                }
                write!(f, ". {})", tail)
            }
            Template::Vector(templates) => {
                write!(f, "#(")?;
                for (i, p) in templates.iter().enumerate() {
                    write!(f, "{}{}", p, if i + 1 == templates.len() { ')' } else { ' ' })?;
                }
                Ok(())
            },
        }
    }
}

impl Rules {
    fn from<P: IntoIterator<Item = Pattern>, T: IntoIterator<Item = Template>>(patterns: P, templates: T) -> Self {
        Self(std::iter::zip(patterns, templates).collect())
    }
}
impl std::fmt::Display for Rules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut literals = std::collections::HashSet::new();
        for (pattern, _) in self.0.iter() {
            literals.extend(pattern.literals());
        }

        write!(f, "(syntax-rules (")?;
        for (i, t) in literals.iter().enumerate() {
            write!(f, "{}{}", t, if i + 1 == literals.len() { ')' } else { ' ' })?;
        }
        write!(f, "\n")?;
        for (i, (pattern, template)) in self.0.iter().enumerate() {
            write!(f, " ({}\n  {}){}", pattern, template, if i + 1 == self.0.len() { ")" } else { "\n" })?;
        }
        Ok(())
    }
}

impl<'p> Environment<'p> {
    fn new_variable(&self) -> usize {
        self.parent_env.new_variable()
    }
    fn new_scope(&self) -> usize {
        let scope = self.scope_counter.get();
        self.scope_counter.set(scope + 1);
        scope
    }
    fn resolve(&self, stx: &Rc<SyntaxObject>) -> Result<Option<Binding>, Error> {
        let (name, mut candidates) = match &stx.e {
            Datum::Symbol(name) => match self.bindings.get(name.as_ref()) {
                Some(bindings) => (name, bindings.iter().filter(|(c_scopes, _)| c_scopes.is_subset(&stx.scopes))),
                None => return Ok(None),
            },
            _ => return Ok(None),
        };

        let (best_scopes, best_binding) = match candidates.clone().max_by_key(|(c_scopes, _)| c_scopes.len()) {
            Some(s) => s,
            None => return Ok(None),
        };
        if candidates.all(|(c_scopes, _)| c_scopes.is_subset(best_scopes)) {
            Ok(Some(*best_binding))
        } else {
            Err(Error::AmbiguousBinding(name.clone()))
        }
    }

    fn parse_syntax_definition(stx: Rc<SyntaxObject>) -> Result<Option<(Cow<'static, str>, Vec<Pattern>, Vec<Template>)>, Error> {
        if let Some(matches) = SYNTAX_DEFINITION_PATTERN.try_match(&stx) {
            let name = match matches.get("name") {
                Some(MatchValue::One(name_stx)) => {
                    if let Datum::Symbol(name) = &name_stx.e {
                        name.clone()
                    } else {
                        return Err(Error::BadSyntax(name_stx.clone()));
                    }
                },
                _ => unreachable!(),
            };
            let literals = match matches.get("literals") {
                Some(MatchValue::One(literals_stx)) => {
                    literals_stx.children.iter().map(|c| {
                        if let Datum::Symbol(s) = &c.e {
                            Ok(s.as_ref())
                        } else {
                            Err(Error::BadSyntax(literals_stx.clone()))
                        }
                    }).collect::<Result<std::collections::HashSet<_>, _>>()?
                },
                _ => unreachable!(),
            };
            let patterns = match matches.get("pattern") {
                Some(MatchValue::Many(vs)) => vs.iter().map(|v| match v {
                    MatchValue::One(pattern_stx) => Pattern::from(pattern_stx, &literals).map_err(|_| Error::BadSyntax(pattern_stx.clone())),
                    _ => unreachable!(),
                }).collect::<Result<Vec<_>, _>>()?,
                _ => unreachable!(),
            };
            let templates = match matches.get("template") {
                Some(MatchValue::Many(vs)) => vs.iter().map(|v| match v {
                    MatchValue::One(template_stx) => Template::from(template_stx).map_err(|_| Error::BadSyntax(stx.clone())),
                    _ => unreachable!(),
                }).collect::<Result<Vec<_>, _>>()?,
                _ => unreachable!(),
            };
            if patterns.len() == templates.len() {
                eprintln!("macro {}: rules={:#?}", name, std::iter::zip(patterns.iter().cloned(), templates.iter().cloned()).collect::<Vec<_>>());
                Ok(Some((name, patterns, templates)))
            } else {
                Err(Error::BadSyntax(stx))
            }
        } else {
            Ok(None)
        }
    }

    fn expand_qq_syntax(&self, mut stx: Rc<SyntaxObject>, qq_depth: usize) -> Result<(Rc<SyntaxObject>, bool), Error> {
        if qq_depth == 0 {
            return self.expand_syntax_once(stx);
        }

        if matches!(stx.e, Datum::List | Datum::ImproperList) {
            let head = match stx.children.first() {
                None => return Ok((stx, false)),
                Some(head) => self.resolve(head)?,
            };
            if matches!(head, Some(Binding::CoreForm(CoreForm::Quasiquote))) {
                let (child, modified) = self.expand_qq_syntax(stx.children[1].clone(), qq_depth + 1)?;
                if modified {
                    Rc::make_mut(&mut stx).children[1] = child;
                }
                return Ok((stx, modified));
            }
            if matches!(head, Some(Binding::CoreForm(CoreForm::Unquote | CoreForm::UnquoteSplicing))) {
                let (child, modified) = self.expand_qq_syntax(stx.children[1].clone(), qq_depth - 1)?;
                if modified {
                    Rc::make_mut(&mut stx).children[1] = child;
                }
                return Ok((stx, modified));
            }
        }

        if stx.children.len() > 0 {
            let qq_depth = match &stx.e {
                Datum::Quasiquote => qq_depth + 1,
                Datum::Unquote | Datum::UnquoteSplicing => qq_depth - 1,
                _ => qq_depth,
            };

            let mut children = Vec::with_capacity(stx.children.len());
            let mut any_modified = false;
            for c in &stx.children {
                let (child, modified) = self.expand_qq_syntax(c.clone(), qq_depth)?;
                children.push(child);
                any_modified |= modified;
            }
            if any_modified {
                Rc::make_mut(&mut stx).children = children;
            }
            Ok((stx, any_modified))
        } else {
            Ok((stx, false))
        }
    }
    fn expand_syntax_once(&self, mut stx: Rc<SyntaxObject>) -> Result<(Rc<SyntaxObject>, bool), Error> {
        if matches!(stx.e, Datum::List | Datum::ImproperList) {
            let head = match stx.children.first() {
                None => return Ok((stx, false)),
                Some(head) => self.resolve(head)?,
            };
            if let Some(Binding::SyntaxTransformer(index)) = head {
                return self.parent_env.macros.get(index).unwrap().0.iter()
                    .find_map(|(pattern, template)| pattern.try_match(&stx).map(|e| (e, template)))
                    .map(|(env, template)| {
                        let slice = env.iter().map(|(k, v)| (*k, v)).collect();
                        template.apply(&slice, &stx, self.new_scope())
                    })
                    .ok_or(Error::NoMatchingPattern(stx))?
                    .map(|stx| (stx, true));
            } else if matches!(head, Some(Binding::CoreForm(CoreForm::Quasiquote))) {
                let (child, modified) = self.expand_qq_syntax(stx.children[1].clone(), 1)?;
                if modified {
                    Rc::make_mut(&mut stx).children[1] = child;
                }
                return Ok((stx, modified));
            } else if matches!(head, Some(Binding::CoreForm(CoreForm::Quote))) {
                return Ok((stx, false));
            }
        } else if matches!(stx.e, Datum::Quote) {
            return Ok((stx, false));
        } else if matches!(stx.e, Datum::Quasiquote) {
            let (child, modified) = self.expand_qq_syntax(stx.children[0].clone(), 1)?;
            if modified {
                Rc::make_mut(&mut stx).children[0] = child;
            }
            return Ok((stx, modified));
        }

        if stx.children.len() > 0 {
            let mut children = Vec::with_capacity(stx.children.len());
            let mut any_modified = false;
            for c in &stx.children {
                let (child, modified) = self.expand_syntax_once(c.clone())?;
                children.push(child);
                any_modified |= modified;
            }
            if any_modified {
                Rc::make_mut(&mut stx).children = children;
            }
            Ok((stx, any_modified))
        } else {
            Ok((stx, false))
        }
    }
    fn add_binding_scopes(&mut self, stx: &mut SyntaxObject) -> Result<(), Error> {
        if (stx.e == Datum::List || stx.e == Datum::ImproperList) && stx.children.len() > 0 {
            match self.resolve(stx.children.first().unwrap())? {
                Some(Binding::CoreForm(CoreForm::Lambda)) => {
                    let bind_scope = self.new_scope();

                    // Add the binding scope to every identifier in this binding construct
                    stx.children.iter_mut()
                        .skip(1)
                        .for_each(|c| Rc::make_mut(c).add_scope(bind_scope));

                    // Record the binding for each new bound variable
                    match stx.children.get(1).map(|c| &c.e) {
                        None => return Err(Error::BadCoreFormSyntax(CoreForm::Lambda, stx.children[1].clone())),
                        Some(Datum::Symbol(name)) => {
                            let b = Binding::Variable(self.new_variable());
                            let bindings = match self.bindings.get_mut(name.as_ref()) {
                                Some(bb) => bb,
                                None => self.bindings.entry(name.clone()).or_default(),
                            };
                            bindings.insert(stx.children[1].scopes.clone(), b);
                        },
                        Some(Datum::List) | Some(Datum::ImproperList) => {
                            stx.children.get(1).unwrap()
                                .children.iter()
                                .filter_map(|c| match &c.e {
                                    Datum::Symbol(name) => Some((name, &c.scopes)),
                                    _ => None,
                                }).for_each(|(name, scopes)| {
                                    let b = Binding::Variable(self.new_variable());
                                    let bindings = match self.bindings.get_mut(name.as_ref()) {
                                        Some(bb) => bb,
                                        None => self.bindings.entry(name.clone()).or_default(),
                                    };
                                    bindings.insert(scopes.clone(), b);
                                });
                            },
                        Some(_) => return Err(Error::BadCoreFormSyntax(CoreForm::Lambda, stx.children[1].clone())),
                    };

                    // Add binding scopes to any children of this expression
                    stx.children.iter_mut()
                        .map(|c| self.add_binding_scopes(Rc::make_mut(c)))
                        .collect::<Result<(), _>>()?;
                },
                Some(Binding::CoreForm(CoreForm::Begin)) => {
                    let bind_scope = self.new_scope(); // This is needed for any definitions in the begin block.
                    stx.children.iter_mut()
                        .skip(1)
                        .for_each(|c| Rc::make_mut(c).add_scope(bind_scope));
                    stx.children.iter_mut()
                        .map(|c| self.add_binding_scopes(Rc::make_mut(c)))
                        .collect::<Result<(), _>>()?;
                },
                Some(Binding::CoreForm(CoreForm::Quote)) => (),
                _ => {
                    stx.children.iter_mut()
                        .map(|c| self.add_binding_scopes(Rc::make_mut(c)))
                        .collect::<Result<(), _>>()?;
                },
            }
        }
        Ok(())
    }
    fn expand_and_scope(&mut self, mut stx: Rc<SyntaxObject>) -> Result<Rc<SyntaxObject>, Error> {
        let mut expanded = loop {
            match self.expand_syntax_once(stx)? {
                (ns, true) => stx = ns,
                (ns, false) => break ns,
            }
        };

        self.add_binding_scopes(Rc::make_mut(&mut expanded))?;
        Ok(expanded)
    }

    fn define_inner(&mut self, mut stx: Rc<SyntaxObject>) -> Result<Rc<SyntaxObject>, Error> {
        if let Some((name, patterns, templates)) = Self::parse_syntax_definition(stx.clone())? {
            match self.bindings.get_mut(name.as_ref()) {
                Some(bb) => bb.insert(stx.scopes.clone(), Binding::SyntaxTransformer(self.parent_env.macros.len())).map(|_| ()),
                None => self.bindings.insert(name, std::iter::once((stx.scopes.clone(), Binding::SyntaxTransformer(self.parent_env.macros.len()))).collect()).map(|_| ()),
            };
            self.parent_env.macros.push_back(Rules::from(patterns, templates));
            Ok(Rc::new(SyntaxObject::simple(Datum::Boolean(true), stx.source_location)))
        } else if let Some(matches) = VALUE_DEFINITION_PATTERN.try_match(&stx) {
            if let Some(MatchValue::Many(vs)) = matches.get("formals") {
                for v in vs {
                    if let MatchValue::One(stx) = v {
                        let b = Binding::Variable(self.new_variable());
                        match &**stx {
                            SyntaxObject { e: Datum::Symbol(name), scopes, ..} => {
                                let bindings = match self.bindings.get_mut(name.as_ref()) {
                                    Some(bb) => bb,
                                    None => self.bindings.entry(name.clone()).or_default(),
                                };
                                bindings.insert(scopes.clone(), b);
                            },
                            _ => (),
                        }
                    }
                }
                Ok(stx)
            } else {
                unreachable!()
            }
        } else if let Some(matches) = SYNTAX_BINDING_PATTERN.try_match(&stx) {
            let bind_scope = self.new_scope();
            let body_scopes: ScopeSet = stx.scopes.iter().map(|sc| *sc).chain(std::iter::once(bind_scope)).collect();
            let names = match matches.get("name") {
                Some(MatchValue::Many(vs)) => vs.iter().map(|v| match v {
                    MatchValue::One(stx) => match &**stx {
                        SyntaxObject { e: Datum::Symbol(name), .. } => Ok(name.clone()),
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }).collect::<Result<Vec<_>, _>>()?,
                _ => unreachable!(),
            };
            for (i, name) in names.iter().enumerate() {
                let bindings = match self.bindings.get_mut(name.as_ref()) {
                    Some(bb) => bb,
                    None => self.bindings.entry(name.clone()).or_default(),
                };
                bindings.insert(body_scopes.clone(), Binding::SyntaxTransformer(self.parent_env.macros.len() + i));
            }

            let literals = match matches.get("literals") {
                Some(MatchValue::Many(vs)) => vs.iter().map(|v| match v {
                    MatchValue::One(literals_stx) => literals_stx.children.iter()
                        .map(|c| if let Datum::Symbol(s) = &c.e {
                            Ok(s.as_ref())
                        } else {
                            Err(Error::BadSyntax(literals_stx.clone()))
                        }).collect::<Result<std::collections::HashSet<_>, Error>>(),
                    _ => unreachable!(),
                }).collect::<Result<Vec<_>, Error>>()?,
                _ => unreachable!(),
            };
            let patternses = match matches.get("pattern") {
                Some(MatchValue::Many(vs)) => vs.iter().enumerate().map(|(i, v)| match v {
                    MatchValue::Many(vs) => vs.iter().map(|v| match v {
                        MatchValue::One(pattern_stx) => Pattern::from(pattern_stx, &literals[i]).map_err(|_| Error::BadSyntax(pattern_stx.clone())),
                        _ => unreachable!(),
                    }).collect::<Result<Vec<_>, Error>>(),
                    _ => unreachable!(),
                }).collect::<Result<Vec<_>, Error>>()?,
                _ => unreachable!(),
            };
            let templateses = match matches.get("template") {
                Some(MatchValue::Many(vs)) => vs.iter().map(|v| match v {
                    MatchValue::Many(vs) => vs.iter().map(|v| match v {
                        MatchValue::One(template_stx) => Template::from(template_stx).map_err(|_| Error::BadSyntax(template_stx.clone())),
                        _ => unreachable!(),
                    }).collect::<Result<Vec<_>, Error>>(),
                    _ => unreachable!(),
                }).collect::<Result<Vec<_>, Error>>()?,
                _ => unreachable!(),
            };
            if names.len() != patternses.len() || patternses.len() != templateses.len() || names.len() != templateses.len() {
                return Err(Error::BadSyntax(stx));
            }
            for (patterns, templates) in std::iter::zip(patternses, templateses) {
                self.parent_env.macros.push_back(Rules::from(patterns, templates));
            }

            let mut bodys = vec![Rc::new(SyntaxObject::simple(Datum::Symbol(Cow::Borrowed("begin")), (0, 0)))];
            bodys.extend(match matches.get("body") {
                Some(MatchValue::Many(vs)) => vs.iter().map(|v| match v {
                    MatchValue::One(stx) => stx.clone(),
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
            bodys.iter_mut().skip(1).for_each(|stx| Rc::make_mut(stx).add_scope(bind_scope));
            Ok(Rc::new(SyntaxObject {
                e: Datum::List,
                children: bodys,
                scopes: body_scopes,
                source_location: (0, 0),
            }))
        } else {
            Rc::make_mut(&mut stx).children = stx.children.iter()
                .map(|stx| self.define_inner(stx.clone()))
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(stx)
        }
    }

    fn qquote_syntax(&self, stx: &Rc<SyntaxObject>, depth: usize) -> Result<Expression, Error> {
        match &stx.e {
            Datum::Boolean(_) | Datum::Integer(_) | Datum::Rational(_, _) | Datum::Float(_) | Datum::Character(_) | Datum::String(_) | Datum::Symbol(_) | Datum::Bytes(_) | Datum::Quote | Datum::Vector => Ok(Expression::Literal(stx.as_ref().into())),
            Datum::Quasiquote => Ok(Expression::ProcedureCall {
                operator: Box::new(Expression::CorePrimitive("list")),
                operands: vec![LiteralD::Static("quasiquote").into(),
                    self.qquote_syntax(stx.children.first().unwrap(), depth + 1)?],
            }),
            Datum::Unquote => if depth == 1 {
                self.resolve_syntax(stx.children.first().unwrap())
            } else {
                Ok(Expression::ProcedureCall {
                    operator: Box::new(Expression::CorePrimitive("list")),
                    operands: vec![LiteralD::Static("unquote").into(),
                        self.qquote_syntax(stx.children.first().unwrap(), depth - 1)?],
                })
            },
            Datum::UnquoteSplicing => if depth == 1 {
                self.resolve_syntax(stx.children.first().unwrap())
            } else {
                Ok(Expression::ProcedureCall {
                    operator: Box::new(Expression::CorePrimitive("list")),
                    operands: vec![LiteralD::Static("unquote-splicing").into(),
                        self.qquote_syntax(stx.children.first().unwrap(), depth - 1)?],
                })
            },
            Datum::ImproperList => {
                match self.qquote_syntax_list(stx, depth)? {
                    Expression::Literal(Literal::NoCopy(LiteralD::List(mut ee, None))) => {
                        let tail = ee.pop().map(Box::new);
                        Ok(LiteralD::List(ee, tail).into())
                    },
                    Expression::ProcedureCall { operator, mut operands } => match *operator {
                        Expression::CorePrimitive("list") => if let Some(tail) = operands.pop() {
                            Ok(Expression::ProcedureCall {
                                operator: Box::new(Expression::CorePrimitive("append")),
                                operands: vec![Expression::ProcedureCall {
                                    operator: Box::new(Expression::CorePrimitive("list")),
                                    operands,
                                }, tail],
                            })
                        } else {
                            Ok(Expression::ProcedureCall { operator, operands })
                        },
                        Expression::CorePrimitive("append") => match operands.pop() {
                            None => Ok(LiteralC::Nil.into()),
                            Some(Expression::ProcedureCall { operator: sub_operator, operands: mut sub_operands }) if matches!(*sub_operator, Expression::CorePrimitive("list")) => {
                                if let Some(tail) = sub_operands.pop() {
                                    operands.push(Expression::ProcedureCall { operator: sub_operator, operands: sub_operands });
                                    operands.push(tail);
                                }
                                Ok(Expression::ProcedureCall { operator, operands })
                            },
                            Some(_) => Err(Error::BadCoreFormSyntax(CoreForm::UnquoteSplicing, stx.clone())),
                        },
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
                // e is either: Literal(List(vec, None)), so we just take the last atom out and stick it in the tail.
                // or ProcedureCall(list, ...), so we just take the last one out and say (append [orig] [tail])
                // or ProcedureCall(append, ...), so we find the last child (which will be a list) and do ^^
            },
            Datum::List => match stx.children.first() {
                None => Ok(LiteralC::Nil.into()),
                Some(head) => match self.resolve(head)? {
                    Some(Binding::CoreForm(CoreForm::Quote)) => match stx.children.get(1) {
                        None => Err(Error::BadCoreFormSyntax(CoreForm::Quote, stx.clone())),
                        Some(stx) => Ok(Expression::Literal(stx.as_ref().into())),
                    },
                    Some(Binding::CoreForm(CoreForm::Quasiquote)) => match stx.children.get(1) {
                        None => Err(Error::BadCoreFormSyntax(CoreForm::Quasiquote, stx.clone())),
                        Some(stx) => Ok(Expression::ProcedureCall {
                            operator: Box::new(Expression::CorePrimitive("list")),
                            operands: vec![LiteralD::Static("quasiquote").into(), self.qquote_syntax(stx.children.first().unwrap(), depth + 1)?],
                        }),
                    },
                    Some(Binding::CoreForm(CoreForm::Unquote)) => match stx.children.get(1) {
                        None => Err(Error::BadCoreFormSyntax(CoreForm::Unquote, stx.clone())),
                        Some(stx) => if depth == 1 {
                            self.resolve_syntax(stx)
                        } else {
                            Ok(Expression::ProcedureCall {
                                operator: Box::new(Expression::CorePrimitive("list")),
                                operands: vec![LiteralD::Static("unquote").into(), self.qquote_syntax(stx, depth - 1)?],
                            })
                        },
                    },
                    Some(Binding::CoreForm(CoreForm::UnquoteSplicing)) => match stx.children.get(1) {
                        None => Err(Error::BadCoreFormSyntax(CoreForm::UnquoteSplicing, stx.clone())),
                        Some(stx) => if depth == 1 {
                            self.resolve_syntax(stx)
                        } else {
                            Ok(Expression::ProcedureCall {
                                operator: Box::new(Expression::CorePrimitive("list")),
                                operands: vec![LiteralD::Static("unquote-splicing").into(), self.qquote_syntax(stx, depth - 1)?],
                            })
                        },
                    },
                    _ => self.qquote_syntax_list(stx, depth),
                }
            }
        }
    }
    fn qquote_syntax_list(&self, stx: &Rc<SyntaxObject>, depth: usize) -> Result<Expression, Error> {
        let mut qqs = false;
        let mut append_items = Vec::new();
        for stx in &stx.children {
            let mut q = None;
            if matches!(stx.e, Datum::List) && stx.children.len() > 1 {
                if let Datum::Symbol(name) = &stx.children.first().unwrap().e {
                    q = match name.as_ref() {
                        "unquote" => Some(QQExpansion::Unquote(self.qquote_syntax(&stx.children[1], depth - 1)?)),
                        "unquote-splicing" => Some(QQExpansion::UnquoteSplicing(self.qquote_syntax(&stx.children[1], depth - 1)?)),
                        _ => None,
                    };
                }
            }
            if q.is_none() {
                q = Some(match (&stx.e, self.qquote_syntax(&stx, depth)?) {
                    (Datum::Unquote, e) => QQExpansion::Unquote(e),
                    (Datum::UnquoteSplicing, e) => QQExpansion::UnquoteSplicing(e),
                    (_, Expression::Literal(e)) => QQExpansion::Literal(e),
                    (_, e) => QQExpansion::Unquote(e),
                });
            }

            qqs = match (qqs, q.unwrap()) {
                (false, QQExpansion::Literal(e)) => {
                    append_items.push(Expression::ProcedureCall {
                        operator: Box::new(Expression::CorePrimitive("list")),
                        operands: vec![Expression::Literal(e)],
                    });
                    true
                },
                (false, QQExpansion::Unquote(e)) => {
                    append_items.push(Expression::ProcedureCall {
                        operator: Box::new(Expression::CorePrimitive("list")),
                        operands: vec![e],
                    });
                    true
                },
                (true, QQExpansion::Literal(e)) => {
                    if let Expression::ProcedureCall { operands, .. } = append_items.last_mut().unwrap() {
                        operands.push(Expression::Literal(e));
                        true
                    } else {
                        unreachable!()
                    }
                },
                (true, QQExpansion::Unquote(e)) => {
                    if let Expression::ProcedureCall { operands, .. } = append_items.last_mut().unwrap() {
                        operands.push(e);
                        true
                    } else {
                        unreachable!()
                    }
                },
                (_, QQExpansion::UnquoteSplicing(e)) => {
                    append_items.push(e);
                    false
                }
            };
        }

        if append_items.len() > 1 {
            Ok(Expression::ProcedureCall {
                operator: Box::new(Expression::CorePrimitive("append")),
                operands: append_items,
            })
        } else {
            match append_items.pop() {
                None => Ok(LiteralC::Nil.into()),
                Some(Expression::ProcedureCall { operands, .. }) if operands.iter().all(|q| matches!(q, Expression::Literal(_))) => {
                    Ok(Expression::Literal(Literal::NoCopy(LiteralD::List(operands.into_iter().map(|q| match q {
                        Expression::Literal(e) => e,
                        _ => unreachable!(),
                    }).collect(), None))))
                },
                Some(e) => Ok(e),
            }
        }
    }
    fn resolve_syntax(&self, stx: &Rc<SyntaxObject>) -> Result<Expression, Error> {
        match &stx.e {
            Datum::Boolean(_) | Datum::Integer(_) | Datum::Rational(_, _) | Datum::Float(_) | Datum::Character(_) | Datum::String(_) | Datum::Bytes(_) | Datum::Vector => Ok(Expression::Literal(stx.as_ref().into())),
            Datum::Quote => Ok(Expression::Literal(stx.children[0].as_ref().into())),
            Datum::Quasiquote => self.qquote_syntax(stx.children.first().unwrap(), 1),
            Datum::Unquote | Datum::UnquoteSplicing => Err(Error::BadSyntax(stx.clone())),
            Datum::Symbol(_) => match self.resolve(&stx)? {
                Some(Binding::CorePrimitive(name)) => Ok(Expression::CorePrimitive(name)),
                Some(Binding::Variable(i)) => Ok(Expression::Variable(i)),
                Some(_) => Err(Error::BadSyntax(stx.clone())),
                None => Err(Error::UnboundIdentifier(stx.clone())),
            },
            Datum::ImproperList => return Err(Error::BadSyntax(stx.clone())),
            Datum::List => match stx.children.first() {
                None => Ok(LiteralC::Nil.into()),
                Some(head) => match self.resolve(head)? {
                    None => match &head.e {
                        Datum::Symbol(_) => Err(Error::UnboundIdentifier(head.clone())),
                        _ => Ok(Expression::ProcedureCall {
                            operator: match self.resolve_syntax(head) {
                                Ok(expr) => Box::new(expr),
                                Err(Error::BadSyntax(_)) => return Err(Error::BadSyntax(stx.clone())),
                                Err(e) => return Err(e),
                            },
                            operands: stx.children.iter().skip(1)
                                .map(|stx| self.resolve_syntax(stx))
                                .collect::<Result<Vec<_>, _>>()?,
                        }),
                    },
                    Some(Binding::SyntaxTransformer(_)) => Err(Error::BadSyntax(stx.clone())),
                    Some(Binding::CorePrimitive(s)) => Ok(Expression::ProcedureCall {
                        operator: Box::new(Expression::CorePrimitive(s)),
                        operands: stx.children.iter().skip(1)
                            .map(|stx| self.resolve_syntax(stx))
                            .collect::<Result<Vec<_>, _>>()? }),
                    Some(Binding::Variable(i)) => Ok(Expression::ProcedureCall {
                        operator: Box::new(Expression::Variable(i)),
                        operands: stx.children.iter().skip(1)
                            .map(|stx| self.resolve_syntax(stx))
                            .collect::<Result<Vec<_>, _>>()? }),
                    Some(Binding::CoreForm(CoreForm::Lambda)) => Ok(Expression::Lambda {
                        formals: match stx.children.get(1) {
                            None => return Err(Error::BadCoreFormSyntax(CoreForm::Lambda, stx.clone())),
                            Some(stx) => match &stx.e {
                                Datum::Symbol(_) => match self.resolve(stx)? {
                                    Some(Binding::Variable(i)) => (Vec::new(), Some(Box::new(Expression::Variable(i)))),
                                    Some(_) => return Err(Error::BadCoreFormSyntax(CoreForm::Lambda, stx.clone())),
                                    None => return Err(Error::UnboundIdentifier(stx.clone())),
                                },
                                Datum::List => (stx.children.iter()
                                    .map(|stx| match self.resolve(stx)? {
                                        Some(Binding::Variable(i)) => Ok(Expression::Variable(i)),
                                        Some(_) => Err(Error::BadCoreFormSyntax(CoreForm::Lambda, stx.clone())),
                                        None => Err(Error::UnboundIdentifier(stx.clone())),
                                    })
                                    .collect::<Result<Vec<_>, _>>()?, None),
                                Datum::ImproperList => {
                                    let mut formals = stx.children.iter()
                                        .map(|stx| match self.resolve(stx)? {
                                            Some(Binding::Variable(i)) => Ok(Expression::Variable(i)),
                                            Some(_) => Err(Error::BadCoreFormSyntax(CoreForm::Lambda, stx.clone())),
                                            None => Err(Error::UnboundIdentifier(stx.clone())),
                                        })
                                        .collect::<Result<Vec<_>, _>>()?;
                                    let tail = formals.pop().map(Box::new);
                                    (formals, tail)
                                },
                                _ => return Err(Error::BadCoreFormSyntax(CoreForm::Lambda, stx.clone())),
                            }
                        },
                        body: stx.children.iter().skip(2)
                            .map(|stx| self.resolve_syntax(stx))
                            .collect::<Result<Vec<_>, _>>()? }),
                    Some(Binding::CoreForm(CoreForm::If)) => Ok(Expression::Conditional {
                        test: match stx.children.get(1) {
                            None => return Err(Error::BadCoreFormSyntax(CoreForm::If, stx.clone())),
                            Some(stx) => Box::new(self.resolve_syntax(stx)?),
                        },
                        consequent: match stx.children.get(2) {
                            None => return Err(Error::BadCoreFormSyntax(CoreForm::If, stx.clone())),
                            Some(stx) => Box::new(self.resolve_syntax(stx)?),
                        },
                        alternate: match stx.children.get(3) {
                            None => None,
                            Some(stx) => Some(Box::new(self.resolve_syntax(stx)?)),
                        }}),
                    Some(Binding::CoreForm(CoreForm::Begin)) => Ok(Expression::Block {
                        body: stx.children.iter()
                            .skip(1)
                            .map(|stx| self.resolve_syntax(stx))
                            .collect::<Result<Vec<_>, _>>()? }),
                    Some(Binding::CoreForm(CoreForm::SetValues)) => Ok(Expression::Assignment {
                        ids: match stx.children.get(1) {
                            None => return Err(Error::BadCoreFormSyntax(CoreForm::SetValues, stx.clone())),
                            Some(stx) => match &stx.e {
                                Datum::Symbol(_) => match self.resolve(stx)? {
                                    Some(Binding::Variable(i)) => (Vec::new(), Some(Box::new(Expression::Variable(i)))),
                                    Some(_) => return Err(Error::BadCoreFormSyntax(CoreForm::SetValues, stx.clone())),
                                    None => return Err(Error::UnboundIdentifier(stx.clone())),
                                },
                                Datum::List => (stx.children.iter()
                                    .map(|stx| match self.resolve(stx)? {
                                        Some(Binding::Variable(i)) => Ok(Expression::Variable(i)),
                                        Some(_) => Err(Error::BadCoreFormSyntax(CoreForm::SetValues, stx.clone())),
                                        None => Err(Error::UnboundIdentifier(stx.clone())),
                                    })
                                    .collect::<Result<Vec<_>, _>>()?, None),
                                Datum::ImproperList => {
                                    let mut formals = stx.children.iter()
                                        .map(|stx| match self.resolve(stx)? {
                                            Some(Binding::Variable(i)) => Ok(Expression::Variable(i)),
                                            Some(_) => Err(Error::BadCoreFormSyntax(CoreForm::SetValues, stx.clone())),
                                            None => Err(Error::UnboundIdentifier(stx.clone())),
                                        })
                                        .collect::<Result<Vec<_>, _>>()?;
                                    let tail = formals.pop().map(Box::new);
                                    (formals, tail)
                                },
                                _ => return Err(Error::BadCoreFormSyntax(CoreForm::SetValues, stx.clone())),
                            }
                        },
                        value: match stx.children.get(2) {
                            None => return Err(Error::BadCoreFormSyntax(CoreForm::SetValues, stx.clone())),
                            Some(stx) => self.resolve_syntax(stx).map(Box::new)?,
                        }
                    }),
                    Some(Binding::CoreForm(CoreForm::Quote)) => match stx.children.get(1) {
                        None => return Err(Error::BadCoreFormSyntax(CoreForm::Quote, stx.clone())),
                        Some(stx) => Ok(Expression::Literal(stx.as_ref().into())),
                    },
                    Some(Binding::CoreForm(CoreForm::Quasiquote)) => match stx.children.get(1) {
                        None => return Err(Error::BadCoreFormSyntax(CoreForm::Quasiquote, stx.clone())),
                        Some(stx) => self.qquote_syntax(stx, 1),
                    },
                    Some(Binding::CoreForm(CoreForm::Unquote | CoreForm::UnquoteSplicing)) => Err(Error::BadSyntax(stx.clone())),
                    Some(Binding::CoreForm(CoreForm::DefineValues)) => Ok(Expression::Definition {
                        // this is pretty much identical to set-values!
                        ids: match stx.children.get(1) {
                            None => return Err(Error::BadCoreFormSyntax(CoreForm::DefineValues, stx.clone())),
                            Some(stx) => match &stx.e {
                                Datum::Symbol(_) => match self.resolve(stx)? {
                                    Some(Binding::Variable(i)) => (Vec::new(), Some(Box::new(Expression::Variable(i)))),
                                    Some(_) => return Err(Error::BadCoreFormSyntax(CoreForm::DefineValues, stx.clone())),
                                    None => return Err(Error::UnboundIdentifier(stx.clone())),
                                },
                                Datum::List => (stx.children.iter()
                                    .map(|stx| match self.resolve(stx)? {
                                        Some(Binding::Variable(i)) => Ok(Expression::Variable(i)),
                                        Some(_) => Err(Error::BadCoreFormSyntax(CoreForm::DefineValues, stx.clone())),
                                        None => Err(Error::UnboundIdentifier(stx.clone())),
                                    })
                                    .collect::<Result<Vec<_>, _>>()?, None),
                                Datum::ImproperList => {
                                    let mut formals = stx.children.iter()
                                        .map(|stx| match self.resolve(stx)? {
                                            Some(Binding::Variable(i)) => Ok(Expression::Variable(i)),
                                            Some(_) => Err(Error::BadCoreFormSyntax(CoreForm::DefineValues, stx.clone())),
                                            None => Err(Error::UnboundIdentifier(stx.clone())),
                                        })
                                        .collect::<Result<Vec<_>, _>>()?;
                                    let tail = formals.pop().map(Box::new);
                                    (formals, tail)
                                },
                                _ => return Err(Error::BadCoreFormSyntax(CoreForm::DefineValues, stx.clone())),
                            }
                        },
                        value: match stx.children.get(2) {
                            None => return Err(Error::BadCoreFormSyntax(CoreForm::DefineValues, stx.clone())),
                            Some(stx) => self.resolve_syntax(stx).map(Box::new)?,
                        }
                    }),
                },
            },
        }
    }
}

/******** top-level expander ********/
pub fn expand(stx: SyntaxObject, common_env: &mut CommonEnvironment) -> Result<Expression, Error> {
    let mut env = Environment::from(common_env);
    let stx = env.expand_and_scope(Rc::new(stx))?;
    let stx = env.define_inner(stx)?;
    let stx = env.expand_and_scope(stx)?;
    eprintln!("expanded: {}", stx);

    let e = env.resolve_syntax(&stx)?;
    env.parent_env.bound_variables.extend(env.bindings.iter()
        .map(|(_, b)| b.values())
        .flatten()
        .filter_map(|b| if let Binding::Variable(v_id) = b { Some(*v_id) } else { None }));
    env.parent_env.toplevels.extend(env.bindings.into_iter()
        .filter_map(|(name, bindings)| bindings.get(&ScopeSet::default()).map(|b| (name, *b))));
    Ok(e)
}

/******** parser ********/

pub fn read(port: &mut InputPort) -> Result<SyntaxObject, Error> {
    read_datum(port, None)
}

fn read_datum(port: &mut InputPort, token: Option<(Token, (usize, usize))>) -> Result<SyntaxObject, Error> {
    let (token, source_location) = match token {
        Some((t, l)) => (t, l),
        None => match next_token(port).map_err(Error::IoError)? {
            Some((t, l)) => (t, l),
            None => return Err(Error::NoToken),
        }
    };
    match token {
        Token::Boolean(b) => Ok(SyntaxObject::simple(Datum::Boolean(b), source_location)),
        Token::Number(s, st) => Ok(SyntaxObject::simple(parse_number(s, st)?, source_location)),
        Token::Character(s) => {
            let c = parse_character(s).map_err(|_| Error::InvalidCharacter)?;
            Ok(SyntaxObject::simple(Datum::Character(c), source_location))
        },
        Token::String(s) => {
            let s = parse_string(s).map_err(|_| Error::InvalidCharacter)?;
            Ok(SyntaxObject::simple(Datum::String(s), source_location))
        },
        Token::Identifier(s) => {
            let s = parse_identifier(s).map_err(|_| Error::InvalidCharacter)?;
            Ok(SyntaxObject::simple(Datum::Symbol(Cow::Owned(s)), source_location))
        },
        Token::Open => {
            let mut children = Vec::new();
            loop {
                match next_token(port).map_err(Error::IoError)? {
                    Some((Token::Close, _)) => return Ok(SyntaxObject::compound(Datum::List, children, source_location)),
                    Some((Token::Dot, _)) => {
                        children.push(read_datum(port, None)?);
                        match next_token(port).map_err(Error::IoError)? {
                            Some((Token::Close, _)) => return Ok(SyntaxObject::compound(Datum::ImproperList, children, source_location)),
                            _ => return Err(Error::UnexpectedToken),
                        }
                    },
                    t => children.push(read_datum(port, t)?),
                }
            }
        },
        Token::Vector => {
            let mut children = Vec::new();
            loop {
                match next_token(port).map_err(Error::IoError)? {
                    Some((Token::Close, _)) => return Ok(SyntaxObject::compound(Datum::Vector, children, source_location)),
                    t => children.push(read_datum(port, t)?),
                }
            }
        },
        Token::Bytes => {
            let mut bytes = Vec::new();
            loop {
                match next_token(port).map_err(Error::IoError)? {
                    Some((Token::Close, _)) => return Ok(SyntaxObject::simple(Datum::Bytes(bytes), source_location)),
                    Some((Token::Number(s, TokenTag::Integer), _)) => {
                        match s.parse::<u8>() {
                            Ok(b) => bytes.push(b),
                            Err(_) => return Err(Error::InvalidNumber("expected byte")),
                        }
                    },
                    _ => return Err(Error::UnexpectedToken),
                }
            }
        },
        Token::Quote => Ok(SyntaxObject::compound(Datum::Quote, vec![read_datum(port, None)?], source_location)),
        Token::Backquote => Ok(SyntaxObject::compound(Datum::Quasiquote, vec![read_datum(port, None)?], source_location)),
        Token::Unquote => Ok(SyntaxObject::compound(Datum::Unquote, vec![read_datum(port, None)?], source_location)),
        Token::UnquoteSplicing => Ok(SyntaxObject::compound(Datum::UnquoteSplicing, vec![read_datum(port, None)?], source_location)),
        _ => Err(Error::UnexpectedToken),
    }
}
fn parse_boolean(s: &str) -> Result<bool, ()> {
    match s {
        "#t" | "#true" => Ok(true),
        "#f" | "#false" => Ok(false),
        _ => Err(()),
    }
}
fn parse_number(fs: String, st: TokenTag) -> Result<Datum, Error> {
    let mut s = fs.as_str();
    let mut radix = 10;
    let mut sign = 1;
    if s.starts_with("#b") {
        radix = 2;
        s = &s[2..];
    } else if s.starts_with("#o") {
        radix = 8;
        s = &s[2..];
    } else if s.starts_with("#x") {
        radix = 16;
        s = &s[2..];
    } else if s.starts_with("#d") {
        radix = 10;
        s = &s[2..];
    }

    if s.starts_with('+') {
        sign = 1;
        s = &s[1..];
    } else if s.starts_with('-') {
        sign = -1;
        s = &s[1..];
    }

    match st {
        TokenTag::Integer => {
            match i64::from_str_radix(s, radix) {
                Ok(n) => Ok(Datum::Integer(sign * n)),
                Err(e) => match e.kind() {
                    std::num::IntErrorKind::Zero => Ok(Datum::Integer(0)),
                    std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                        match s.parse::<f32>() {
                            Ok(f) => Ok(Datum::Float(sign as f32 * f)),
                            Err(_) => Err(Error::InvalidNumber("illegal number")),
                        }
                    },
                    _ => Err(Error::InvalidNumber("illegal number")),
                }
            }
        }
        TokenTag::Rational => {
            let mut parts = s.splitn(2, '/');
            if let (Some(nstr), Some(dstr)) = (parts.next(), parts.next()) {
                match (i64::from_str_radix(nstr.trim(), radix), i64::from_str_radix(dstr.trim(), radix)) {
                    (Ok(n), Ok(d)) => {
                        if d == 0 {
                            Err(Error::InvalidNumber("division by zero"))
                        } else {
                            Ok(Datum::Rational(sign * n, d))
                        }
                    },
                    _ => Err(Error::InvalidNumber("illegal number")),
                }
            } else {
                Err(Error::InvalidNumber("illegal number"))
            }
        }
        TokenTag::Decimal => match s.parse::<f32>() {
            Ok(f) => Ok(Datum::Float(sign as f32 * f)),
            Err(_) => Err(Error::InvalidNumber("illegal number")),
        },
        TokenTag::Infinity => Ok(Datum::Float(sign as f32 * f32::INFINITY)),
        TokenTag::NaN => Ok(Datum::Float(sign as f32 * f32::NAN)),
    }
}
fn parse_character(s: String) -> Result<char, ()> {
    match s.as_str() {
        "#\\alarm" => Ok('\u{0007}'),
        "#\\backspace" => Ok('\u{0008}'),
        "#\\delete" => Ok('\u{007F}'),
        "#\\escape" => Ok('\u{001B}'),
        "#\\newline" => Ok('\n'),
        "#\\return" => Ok('\r'),
        "#\\space" => Ok(' '),
        "#\\tab" => Ok('\t'),
        _ => {
            let mut cit = s.chars().skip(2);
            match cit.next().unwrap() {
                'x' => {
                    u32::from_str_radix(&s[3..], 16)
                        .ok()
                        .and_then(char::from_u32)
                        .ok_or(())
                },
                c => Ok(c),
            }
        }
    }
}
fn parse_string(rstr: String) -> Result<String, ()> {
    let mut ci = rstr.chars().skip(1).peekable();
    let mut s = String::with_capacity(rstr.len() - 2);
    while let Some(c) = ci.next() {
        match c {
            '"' => return Ok(s),
            '\\' => {
                match ci.next().ok_or(())? {
                    'a' => s.push('\u{0007}'),
                    'b' => s.push('\u{0008}'),
                    'n' => s.push('\n'),
                    'r' => s.push('\r'),
                    't' => s.push('\t'),
                    '"' => s.push('"'),
                    '\\' => s.push('\\'),
                    'x' => {
                        let mut ccode = 0;
                        while let Some(digit) = ci.next_if(|c| c.is_digit(16)) {
                            ccode = ccode * 16 + digit.to_digit(16).unwrap();
                        }
                        if ci.next().unwrap() == ';' {
                            s.push(unsafe { char::from_u32_unchecked(ccode)});
                        }
                    },
                    ' ' | '\t' | '\n' | '\r' => {
                        while let Some(c) = ci.next() {
                            if c != ' ' && c != '\t' {
                                break;
                            }
                        }
                        match ci.next().ok_or(())? {
                            '\n' => s.push('\n'),
                            '\r' => {
                                s.push('\r');
                                if ci.peek().is_some_and(|&c| c == '\n') {
                                    s.push('\n');
                                    ci.next();
                                }
                            },
                            _ => return Err(()),
                        }
                        while let Some(c) = ci.next() {
                            if c != ' ' && c != '\t' {
                                break;
                            }
                        }
                    },
                    _ => return Err(()),
                };
            },
            _ => s.push(c),
        }
    }
    Err(())
}
fn parse_identifier(rstr: String) -> Result<String, ()> {
    if rstr.starts_with('|') {
        let mut ci = rstr.chars().skip(1).peekable();
        let mut s = String::new();
        while let Some(c) = ci.next() {
            match c {
                '|' => return Ok(s),
                '\\' => {
                    match ci.next().ok_or(())? {
                        'a' => s.push('\u{0007}'),
                        'b' => s.push('\u{0008}'),
                        'n' => s.push('\n'),
                        'r' => s.push('\r'),
                        't' => s.push('\t'),
                        '|' => s.push('|'),
                        '\\' => s.push('\\'),
                        'x' => {
                            let mut ccode = 0;
                            while let Some(digit) = ci.next_if(|c| c.is_digit(16)) {
                                ccode = ccode * 16 + digit.to_digit(16).unwrap();
                            }
                            if ci.next().unwrap() == ';' {
                                s.push(unsafe { char::from_u32_unchecked(ccode)});
                            }
                        },
                        _ => return Err(()),
                    };
                },
                _ => s.push(c),
            }
        }
        Err(())
    } else {
        Ok(rstr)
    }
}

fn next_token(port: &mut InputPort) -> Result<Option<(Token, (usize, usize))>, std::io::Error> {
    while let Some(dh) = atmosphere(port, 0)? {
        port.advance(dh);
    }

    let source_location = port.location();
    if let Some(r) = boolean(port, 0)? {
        if r > 0 {
            return Ok(Some((Token::Boolean(parse_boolean(port.advance(r)).unwrap()), source_location)));
        }
    }
    if let Some((r, st)) = number(port, 0)? {
        if r > 0 {
            return Ok(Some((Token::Number(port.advance(r).to_string(), st), source_location)));
        }
    }
    if let Some(r) = character(port, 0)? {
        if r > 0 {
            return Ok(Some((Token::Character(port.advance(r).to_string()), source_location)));
        }
    }
    if let Some(r) = string(port, 0)? {
        if r > 0 {
            return Ok(Some((Token::String(port.advance(r).to_string()), source_location)));
        }
    }
    if let Some(r) = identifier(port, 0)? {
        if r > 0 {
            return Ok(Some((Token::Identifier(port.advance(r).to_string()), source_location)));
        }
    }
    if port.test("(", 0)? {
        port.advance(1);
        return Ok(Some((Token::Open, source_location)));
    }
    if port.test(")", 0)? {
        port.advance(1);
        return Ok(Some((Token::Close, source_location)));
    }
    if port.test("#(", 0)? {
        port.advance(2);
        return Ok(Some((Token::Vector, source_location)));
    }
    if port.test("#u8(", 0)? {
        port.advance(4);
        return Ok(Some((Token::Bytes, source_location)));
    }
    if port.test("'", 0)? {
        port.advance(1);
        return Ok(Some((Token::Quote, source_location)));
    }
    if port.test("`", 0)? {
        port.advance(1);
        return Ok(Some((Token::Backquote, source_location)));
    }
    if port.test(",@", 0)? {
        port.advance(2);
        return Ok(Some((Token::UnquoteSplicing, source_location)));
    }
    if port.test(",", 0)? {
        port.advance(1);
        return Ok(Some((Token::Unquote, source_location)));
    }
    if port.test(".", 0)? {
        if let Some(_) = delimiter(port, 1)? {
            port.advance(1);
            return Ok(Some((Token::Dot, source_location)));
        }
    }

    Ok(None)
}
fn delimiter(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = whitespace(port, offset)? {
        Ok(Some(r))
    } else {
        match port.peek(offset)? {
            Some(b'(') | Some(b')') | Some(b'"') | Some(b';') | Some(b'|') => Ok(Some(1)),
            _ => Ok(None),
        }
    }
}
fn intraline_whitespace(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test(" ", offset)? || port.test("\t", offset)? {
        Ok(Some(1))
    } else {
        Ok(None)
    }
}
fn whitespace(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = intraline_whitespace(port, offset)? {
        Ok(Some(r))
    } else if let Some(r) = line_ending(port, offset)? {
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn line_ending(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test("\n", offset)? {
        Ok(Some(1))
    } else if port.test("\r\n", offset)? {
        Ok(Some(2))
    } else if port.test("\r", offset)? {
        Ok(Some(1))
    } else {
        Ok(None)
    }
}
fn comment(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = nested_comment(port, offset)? {
        Ok(Some(r))
    } else if port.peek(offset)? == Some(b';') {
        let mut r = 1;
        while line_ending(port, offset + r)?.is_none() {
            r += 1;
        }
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn nested_comment(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test("#|", offset)? {
        let mut r = 2 + comment_text(port, offset + 2)?.unwrap_or(0);
        while let Some(dr) = comment_cont(port, offset + r)? {
            r += dr;
            if dr == 0 {
                break;
            }
        }
        if port.test("|#", offset + r)? {
            return Ok(Some(r + 2));
        }
    }
    Ok(None)
}
fn comment_text(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    let mut r = 0;
    while !port.test("#|", offset + r)? && !port.test("|#", offset + r)? {
        r += 1;
    }
    Ok(Some(r))
}
fn comment_cont(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = nested_comment(port, offset)? {
        Ok(Some(r + comment_text(port, offset + r)?.unwrap_or(0)))
    } else {
        Ok(None)
    }
}
fn atmosphere(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = comment(port, offset)? {
        Ok(Some(r))
    } else if let Some(r) = whitespace(port, offset)? {
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn identifier(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(mut r) = initial(port, offset)? {
        while let Some(dr) = subsequent(port, offset + r)? {
            r += dr;
        }
        if let Some(_) = delimiter(port, offset + r)? {
            return Ok(Some(r));
        }
    }
    if let Some(r) = peculiar_identifier(port, offset)? {
        return Ok(Some(r));
    }
    if port.test("|", offset)? {
        let mut r = 1;
        while let Some(dr) = symbol_element(port, offset + r)? {
            r += dr;
        }
        if port.test("|", offset + r)? {
            return Ok(Some(r + 1));
        }
    }
    Ok(None)
}
fn initial(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'a'..=b'z') | Some(b'A'..=b'Z') => Ok(Some(1)),
        Some(b'!') | Some(b'$') | Some(b'%') | Some(b'&') => Ok(Some(1)),
        Some(b'*') | Some(b'/') | Some(b':') | Some(b'<') => Ok(Some(1)),
        Some(b'=') | Some(b'>') | Some(b'?') | Some(b'^') => Ok(Some(1)),
        Some(b'_') | Some(b'~') => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn subsequent(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = initial(port, offset)? {
        return Ok(Some(r));
    }
    if let Some(r) = digit(port, offset, 10)? {
        return Ok(Some(r));
    }
    match port.peek(offset)? {
        Some(b'+') | Some(b'-') | Some(b'.') | Some(b'@') => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn digit(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'0'..=b'1') if base >= 2 => Ok(Some(1)),
        Some(b'0'..=b'7') if base >= 8 => Ok(Some(1)),
        Some(b'0'..=b'9') if base >= 10 => Ok(Some(1)),
        Some(b'a'..=b'f') if base >= 16 => Ok(Some(1)),
        Some(b'A'..=b'F') if base >= 16 => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn sign(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'+') | Some(b'-') => Ok(Some(1)),
        _ => Ok(Some(0)),
    }
}
fn inline_hex_escape(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test("\\x", offset)? {
        let mut r = 2;
        while let Some(dr) = digit(port, offset + r, 16)? {
            r += dr;
        }
        if port.test(";", offset + r)? {
            return Ok(Some(r + 1));
        }
    }
    Ok(None)
}
fn mnemonic_escape(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'\\') => match port.peek(offset + 1)? {
            Some(b'a') => Ok(Some(1)),
            Some(b'b') => Ok(Some(1)),
            Some(b'n') => Ok(Some(1)),
            Some(b'r') => Ok(Some(1)),
            Some(b't') => Ok(Some(1)),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}
fn peculiar_identifier(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    let r = sign(port, offset)?.unwrap();
    if port.peek(offset + r)? == Some(b'.') {
        if let Some(mut s) = dot_subsequent(port, offset + r + 1)? {
            while let Some(ds) = subsequent(port, offset + r + 1 + s)? {
                s += ds;
            }
            return Ok(Some(r + 1 + s));
        }
    }
    if r > 0 {
        if let Some(mut s) = sign_subsequent(port, offset + r)? {
            while let Some(ds) = subsequent(port, offset + r + s)? {
                s += ds;
            }
            return Ok(Some(r + s));
        } else {
            return Ok(Some(r));
        }
    }
    Ok(None)
}
fn dot_subsequent(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = sign_subsequent(port, offset)? {
        Ok(Some(r))
    } else if port.peek(offset)? == Some(b'.') {
        Ok(Some(1))
    } else {
        Ok(None)
    }
}
fn sign_subsequent(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = initial(port, offset)? {
        return Ok(Some(r));
    }
    match port.peek(offset)? {
        Some(b'+') | Some(b'-') | Some(b'@') => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn symbol_element(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = inline_hex_escape(port, offset)? {
        Ok(Some(r))
    } else if let Some(r) = mnemonic_escape(port, offset)? {
        Ok(Some(r))
    } else {
        match port.peek_char(offset)? {
            Some('|') => Ok(None),
            Some('\\') => {
                match port.peek(offset + 1)? {
                    Some(b'|') => Ok(Some(2)),
                    _ => Ok(None),
                }
            },
            Some(c) => Ok(Some(c.len_utf8())),
            None => Ok(None),
        }
    }
}
fn boolean(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match if port.test("#true", offset)? {
        Some(5)
    } else if port.test("#false", offset)? {
        Some(6)
    } else if port.test("#t", offset)? || port.test("#f", offset)? {
        Some(2)
    } else {
        None
    }
    {
        Some(r) => {
            if let Some(_) = delimiter(port, offset + r)? {
                Ok(Some(r))
            } else {
                Ok(None)
            }
        },
        None => Ok(None),
    }
}
fn character(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.test("#\\", offset)? {
        let r = if port.test("alarm", offset + 2)? {
            Some(7)
        } else if port.test("backspace", offset + 2)? {
            Some(11)
        } else if port.test("delete", offset + 2)? {
            Some(8)
        } else if port.test("escape", offset + 2)? {
            Some(8)
        } else if port.test("newline", offset + 2)? {
            Some(9)
        } else if port.test("return", offset + 2)? {
            Some(8)
        } else if port.test("space", offset + 2)? {
            Some(7)
        } else if port.test("tab", offset + 2)? {
            Some(7)
        } else if port.test("x", offset + 2)? {
            let mut r = 3;
            while let Some(dr) = digit(port, offset + r, 16)? {
                r += dr;
            }
            Some(r)
        } else {
            port.peek_char(offset + 2)?.map(|c| 2 + c.len_utf8())
        };

        if r.is_some_and(|r| r > 0) {
            if delimiter(port, offset + r.unwrap())?.is_some() {
                return Ok(r);
            }
        }
    }
    Ok(None)
}
fn string(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if port.peek(offset)? == Some(b'"'){
        let mut r = 1;
        while let Some(dr) = string_element(port, offset + r)? {
            r += dr;
        }
        if port.peek(offset + r)? == Some(b'"') {
            return Ok(Some(r + 1));
        }
    }
    Ok(None)
}
fn string_element(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(r) = inline_hex_escape(port, offset)? {
        return Ok(Some(r));
    }
    if let Some(r) = mnemonic_escape(port, offset)? {
        return Ok(Some(r));
    }
    if let Some(c) = port.peek_char(offset)? {
        if c == '\\' {
            let nc = port.peek(offset + 1)?;
            if nc == Some(b'"') || nc == Some(b'\\') {
                return Ok(Some(2));
            }

            let mut r = 1;
            while let Some(dr) = intraline_whitespace(port, offset + r)? {
                r += dr;
            }
            if let Some(dr) = line_ending(port, offset + r)? {
                r += dr;
                while let Some(dr) = intraline_whitespace(port, offset + r)? {
                    r += dr;
                }
                return Ok(Some(r));
            }
        } else if c != '"' {
            return Ok(Some(c.len_utf8()));
        }
    }
    Ok(None)
}
fn number(port: &mut InputPort, offset: usize) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
    for base in [2, 8, 10, 16] {
        if let Some(r) = radix(port, offset, base)? {
            if let Some((s, st)) = real(port, offset + r, base)? {
                if let Some(_) = delimiter(port, offset + r + s)? {
                    return Ok(Some((r + s, st)));
                }
            }
        }
    }
    Ok(None)
}
fn real(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
    if let Some((r, st)) = infnan(port, offset)? {
        return Ok(Some((r, st)));
    }
    if let Some(r) = sign(port, offset)? {
        if let Some((s, st)) = ureal(port, offset + r, base)? {
            return Ok(Some((r + s, st)));
        }
    }
    Ok(None)
}
fn ureal(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
    if let Some(r) = decimal(port, offset)? {
        if base == 10 {
            return Ok(Some((r, TokenTag::Decimal)));
        }
    }
    if let Some(r) = uinteger(port, offset, base)? {
        if port.peek(offset + r)? == Some(b'/') {
            if let Some(s) = uinteger(port, offset + r + 1, base)? {
                return Ok(Some((r + s + 1, TokenTag::Rational)));
            }
        } else {
            return Ok(Some((r, TokenTag::Integer)));
        }
    }
    Ok(None)
}
fn decimal(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    if let Some(mut r) = uinteger(port, offset, 10)? {
        if port.peek(offset + r)? == Some(b'.') {
            r += 1;
            while let Some(dr) = digit(port, offset + r, 10)? {
                r += dr;
            }
            if let Some(s) = suffix(port, offset + r)? {
                return Ok(Some(r + s));
            }
        } else if let Some(s) = suffix(port, offset + r)? {
            if s > 0 {
                return Ok(Some(r + s));
            }
        }
    } else if port.peek(offset)? == Some(b'.') {
        let mut r = 1;
        while let Some(dr) = digit(port, offset + r, 10)? {
            r += dr;
        }
        if r > 1 {
            if let Some(s) = suffix(port, offset + r)? {
                return Ok(Some(r + s));
            }
        }
    }
    Ok(None)
}
fn uinteger(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<usize>, std::io::Error> {
    if let Some(mut r) = digit(port, offset, base)? {
        while let Some(dr) = digit(port, offset + r, base)? {
            r += dr;
        }
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn infnan(port: &mut InputPort, offset: usize) -> Result<Option<(usize, TokenTag)>, std::io::Error> {
    if port.test("+inf.0", offset)? || port.test("-inf.0", offset)? {
        Ok(Some((6, TokenTag::Infinity)))
    } else if port.test("+nan.0", offset)? || port.test("-nan.0", offset)? {
        Ok(Some((6, TokenTag::NaN)))
    } else {
        Ok(None)
    }
}
fn suffix(port: &mut InputPort, offset: usize) -> Result<Option<usize>, std::io::Error> {
    match port.peek(offset)? {
        Some(b'e') | Some(b'E') => {
            if let Some(r) = sign(port, offset + 1)? {
                if let Some(s) = uinteger(port, offset + r + 1, 10)? {
                    return Ok(Some(r + s + 1));
                }
            }
            Ok(None)
        },
        _ => Ok(Some(0)),
    }
}
fn radix(port: &mut InputPort, offset: usize, base: i32) -> Result<Option<usize>, std::io::Error> {
    if base == 2 && port.test("#b", offset)? {
        Ok(Some(2))
    } else if base == 8 && port.test("#b", offset)? {
        Ok(Some(2))
    } else if base == 10 {
        if port.test("#d", offset)? {
            Ok(Some(2))
        } else {
            Ok(Some(0))
        }
    } else if base == 16 && port.test("#x", offset)? {
        Ok(Some(2))
    } else {
        Ok(None)
    }
}

/******** built-in macros ********/
pub(crate) fn core_macros() -> (Vec<&'static str>, Vec<Rules>) {
    [
        ("and", Rules(vec![
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("and"))]),
                Template::Constant(Rc::new(SyntaxObject::simple(Datum::Boolean(true), (0, 0)))),
            ),
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("and")), Pattern::Variable(Cow::Borrowed("test"))]),
                Template::Identifier(Cow::Borrowed("test")),
            ),
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("and")), Pattern::Variable(Cow::Borrowed("test1")), Pattern::Ellipsis(Box::new(Pattern::Variable(Cow::Borrowed("test2"))))]),
                Template::List(vec![
                    Template::Identifier(Cow::Borrowed("if")),
                    Template::Identifier(Cow::Borrowed("test1")),
                    Template::List(vec![
                        Template::Identifier(Cow::Borrowed("and")),
                        Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("test2")))),
                    ]),
                    Template::Constant(Rc::new(SyntaxObject::simple(Datum::Boolean(false), (0, 0)))),
                ]),
            ),
        ])),
        ("or", Rules(vec![
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("or"))]),
                Template::Constant(Rc::new(SyntaxObject::simple(Datum::Boolean(false), (0, 0)))),
            ),
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("or")), Pattern::Variable(Cow::Borrowed("test"))]),
                Template::Identifier(Cow::Borrowed("test")),
            ),
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("or")), Pattern::Variable(Cow::Borrowed("test1")), Pattern::Ellipsis(Box::new(Pattern::Variable(Cow::Borrowed("test2"))))]),
                Template::List(vec![
                    Template::Identifier(Cow::Borrowed("let")),
                    Template::List(vec![Template::List(vec![Template::Identifier(Cow::Borrowed("x")), Template::Identifier(Cow::Borrowed("test1"))])]),
                    Template::List(vec![
                        Template::Identifier(Cow::Borrowed("if")),
                        Template::Identifier(Cow::Borrowed("x")),
                        Template::Identifier(Cow::Borrowed("x")),
                        Template::List(vec![
                            Template::Identifier(Cow::Borrowed("or")),
                            Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("test2")))),
                        ]),
                    ]),
                ]),
            ),
        ])),
        ("let", Rules(vec![
            (
                Pattern::List(vec![
                    Pattern::Literal(Cow::Borrowed("let")),
                    Pattern::List(vec![Pattern::Ellipsis(Box::new(Pattern::List(vec![Pattern::Variable(Cow::Borrowed("name")), Pattern::Variable(Cow::Borrowed("val"))])))]),
                    Pattern::Variable(Cow::Borrowed("body1")),
                    Pattern::Ellipsis(Box::new(Pattern::Variable(Cow::Borrowed("body2")))),
                ]),
                Template::List(vec![
                    Template::List(vec![
                        Template::Identifier(Cow::Borrowed("lambda")),
                        Template::List(vec![Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("name"))))]),
                        Template::Identifier(Cow::Borrowed("body1")),
                        Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("body2")))),
                    ]),
                    Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("val")))),
                ]),
            )
        ])),
    ].into_iter().collect()
}
