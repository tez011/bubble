mod frontend;
mod normalize;
mod primitives;
mod syntax;

pub use frontend::{Environment as FrontendEnvironment};
pub use normalize::anf::{Expression, Lambda};
pub use normalize::{normalize as normalize_syntax};
pub use syntax::{expand as expand_syntax, read as read_syntax};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Arity {
    Exact(usize),
    AtLeast(usize),
    #[default] Unknown,
}
impl Arity {
    pub fn join(self, other: Self) -> Self {
        use Arity::*;
        match (self, other) {
            (Exact(m), Exact(n)) if m == n => Exact(m),
            (Exact(m), Exact(n)) | (Exact(m), AtLeast(n)) | (AtLeast(m), Exact(n)) | (AtLeast(m), AtLeast(n)) => AtLeast(std::cmp::min(m, n)),
            _ => Unknown,
        }
    }
    pub fn compatible_with(self, other: Self) -> bool {
        use Arity::*;
        match (self, other) {
            (Exact(m), Exact(n)) => m == n,
            (AtLeast(m), Exact(n) | AtLeast(n)) => m <= n,
            (Unknown, _) => true,
            _ => false,
        }
    }
}
impl std::fmt::Display for Arity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Arity::Exact(n) => write!(f, "exactly {}", n),
            Arity::AtLeast(n) => write!(f, "at least {}", n),
            Arity::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Binding {
    CoreForm(frontend::CoreForm),
    CorePrimitive(frontend::CorePrimitive),
    SyntaxTransformer(usize),
    Variable(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueID(usize);
impl ValueID {
    pub fn invalid() -> Self {
        ValueID(0)
    }
}
impl TryFrom<Binding> for ValueID {
    type Error = ();
    fn try_from(value: Binding) -> Result<Self, Self::Error> {
        match value {
            Binding::Variable(i) => Ok(Self(i)),
            _ => Err(()),
        }
    }
}
impl<T> From<&(Vec<T>, Option<ValueID>)> for Arity {
    fn from(value: &(Vec<T>, Option<ValueID>)) -> Self {
        match value.1 {
            None => Arity::Exact(value.0.len()),
            Some(_) => Arity::AtLeast(value.0.len()),
        }
    }
}
impl std::fmt::Display for ValueID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "x{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Copy(LiteralC),
    NoCopy(LiteralD),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralRef<'a> {
    Copy(LiteralC),
    NoCopy(&'a LiteralD),
}
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum LiteralC {
    #[default] Nil,
    Boolean(bool),
    Integer(i64),
    Float(f32),
    Character(char),
}
impl Eq for LiteralC { }
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiteralD {
    Static(&'static str),
    String(String),
    Bytes(Vec<u8>),
    Symbol(String),
    List(Vec<Literal>, Option<Box<Literal>>),
    Vector(Vec<Literal>),
}

impl From<LiteralC> for Literal { fn from(x: LiteralC) -> Self { Literal::Copy(x) } }
impl From<LiteralD> for Literal { fn from(x: LiteralD) -> Self { Literal::NoCopy(x) } }
impl From<LiteralC> for LiteralRef<'_> { fn from(x: LiteralC) -> Self { LiteralRef::Copy(x) } }
impl<'a> From<&'a LiteralD> for LiteralRef<'a> { fn from(x: &'a LiteralD) -> Self { LiteralRef::NoCopy(x) } }
impl<'a> From<&'a Literal> for LiteralRef<'a> {
    fn from(x: &'a Literal) -> Self {
        match x {
            Literal::Copy(x) => LiteralRef::Copy(*x),
            Literal::NoCopy(x) => LiteralRef::NoCopy(x),
        }
    }
}
impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", LiteralRef::from(self))
    }
}
impl std::fmt::Display for LiteralRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralRef::Copy(x) => write!(f, "{}", x),
            LiteralRef::NoCopy(x) => write!(f, "{}", x),
        }
    }
}
impl std::fmt::Display for LiteralC {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralC::Nil => write!(f, "()"),
            LiteralC::Boolean(true) => write!(f, "#t"),
            LiteralC::Boolean(false) => write!(f, "#f"),
            LiteralC::Integer(x) => write!(f, "{}", x),
            LiteralC::Float(x) => write!(f, "{}", x),
            LiteralC::Character(x) => write!(f, "#\\{}", x),
        }
    }
}
impl std::fmt::Display for LiteralD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
                write!(f, "#(")?;
                for (i, l) in literals.iter().enumerate() {
                    if i == 0 { write!(f, "{}", l)?; }
                    else { write!(f, " {}", l)?; }
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Atom {
    Literal(LiteralC),
    Variable(ValueID),
    Core(frontend::CorePrimitive),
}
impl std::fmt::Display for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Atom::*;
        match self {
            Literal(LiteralC::Nil) => write!(f, "'()"),
            Literal(x) => std::fmt::Display::fmt(&x, f),
            Variable(x) => std::fmt::Display::fmt(&x, f),
            Core(x) => std::fmt::Display::fmt(Into::<&'static str>::into(x), f),
        }
    }
}
impl From<Atom> for Binding {
    fn from(atom: Atom) -> Self {
        match atom {
            Atom::Literal(_) => panic!(),
            Atom::Variable(ValueID(x)) => Binding::Variable(x),
            Atom::Core(s) => Binding::CorePrimitive(s),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Location(u32, u32);
impl Location {
    pub fn is_some(self) -> bool { self.0 > 0 }
}
impl Default for Location {
    fn default() -> Self {
        Self(0, 0)
    }
}
impl<T: Into<u32>> From<(T, T)> for Location {
    fn from(value: (T, T)) -> Self {
        Self(value.0.into(), value.1.into())
    }
}
impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}
#[derive(Clone)]
pub struct Located<T> {
    item: T,
    pub(crate) location: Location,
}
impl<T> Located<T> {
    fn new(item: T, location: Location) -> Self {
        Located { item, location }
    }
}
impl<T: Copy> Copy for Located<T> { }
impl<T> From<T> for Located<T> {
    fn from(item: T) -> Self {
        Self::new(item, Default::default())
    }
}
impl<T> std::ops::Deref for Located<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.item
    }
}
impl<T> std::ops::DerefMut for Located<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.item
    }
}
impl<T: PartialEq> PartialEq for Located<T> {
    fn eq(&self, other: &Self) -> bool {
        self.item == other.item
    }
}
impl<T: Eq> Eq for Located<T> { }
impl<T: std::fmt::Display> std::fmt::Display for Located<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.location.is_some() {
            write!(f, "{}:{}:{}", self.item, self.location.0, self.location.1)
        } else {
            self.item.fmt(f)
        }
    }
}
impl<T: std::fmt::Debug> std::fmt::Debug for Located<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.item.fmt(f)
    }
}
