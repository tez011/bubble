mod io;
mod syntax;

#[derive(Debug, Clone, Copy)]
pub enum Binding {
    CoreForm(CoreForm),
    ExpandOnlyCoreForm,
    CorePrimitive,
    SyntaxTransformer(usize),
    Variable(usize),
}

#[repr(u8)] #[derive(Debug, Clone, Copy)]
pub enum CoreForm {
    Lambda,
    If,
    Begin,
    SetBang,
    Quote,
    Quasiquote,
    Unquote,
    UnquoteSplicing,
    DefineValues,
    _MaxValue,
}
impl std::fmt::Display for CoreForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoreForm::Lambda => write!(f, "lambda"),
            CoreForm::If => write!(f, "if"),
            CoreForm::Begin => write!(f, "begin"),
            CoreForm::SetBang => write!(f, "set!"),
            CoreForm::Quote => write!(f, "quote"),
            CoreForm::Quasiquote => write!(f, "quasiquote"),
            CoreForm::Unquote => write!(f, "unquote"),
            CoreForm::UnquoteSplicing => write!(f, "unquote-splicing"),
            CoreForm::DefineValues => write!(f, "define-values"),
            CoreForm::_MaxValue => panic!(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Expression {
    CoreForm(CoreForm),
    CorePrimitive(String),
    Variable(usize),
    Boolean(bool),
    Integer(i64),
    Rational(i64, i64),
    Float(f64),
    Character(char),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Expression>),
    ImproperList(Vec<Expression>, Box<Expression>),
    Vector(Vec<Expression>),
}
impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::CoreForm(cf) => cf.fmt(f),
            Expression::CorePrimitive(s) => s.fmt(f),
            Expression::Variable(x) => write!(f, "x{}", x),
            Expression::Boolean(true) => write!(f, "#t"),
            Expression::Boolean(false) => write!(f, "#f"),
            Expression::Integer(i) => write!(f, "{}", i),
            Expression::Rational(n, d) => write!(f, "{}/{}", n, d),
            Expression::Float(fl) => write!(f, "{}", fl),
            Expression::Character(c) => write!(f, "\\#{}", c),
            Expression::String(s) => write!(f, "\"{}\"", s),
            Expression::Bytes(b) => write!(f, "{:?}", b),
            Expression::List(children) => {
                write!(f, "(")?;
                for (i, child) in children.iter().enumerate() {
                    write!(f, "{}", child)?;
                    if i < children.len() - 1 {
                        write!(f, " ")?;
                    }
                }
                write!(f, ")")
            },
            Expression::ImproperList(children, tail) => {
                write!(f, "(")?;
                for (i, child) in children.iter().enumerate() {
                    write!(f, "{}", child)?;
                    if i < children.len() - 1 {
                        write!(f, " ")?;
                    }
                }
                write!(f, " . {})", tail)
            },
            Expression::Vector(children) => {
                write!(f, "(")?;
                for (i, child) in children.iter().enumerate() {
                    write!(f, "{}", child)?;
                    if i < children.len() - 1 {
                        write!(f, " ")?;
                    }
                }
                write!(f, ")")
            },
        }
    }
}

fn main() {
    let mut stdin = std::io::stdin().lock();
    let mut handle = io::InputPort::try_from(&mut stdin as &mut dyn std::io::Read).unwrap();
    let mut e_env = syntax::Environment::new();
    loop {
        let stx = match syntax::read(&mut handle) {
            Ok(stx) => stx,
            Err(e) => { eprintln!("Parse error at {:?}: {}", handle.location(), e); continue; },
        };
        let se = match syntax::expand(stx, &mut e_env) {
            Ok(se) => se,
            Err(e) => {
                eprintln!("Expand error at {:?}: {}", handle.location(), e);
                continue;
            }
        };
        println!("final form: {:?}", se);
    }
}
