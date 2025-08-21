mod io;
mod syntax;

#[derive(Debug, Clone)]
pub enum Expression {
    CorePrimitive(&'static str),
    Variable(usize),
    Literal(Literal),
    ProcedureCall { operator: Box<Expression>, operands: (Vec<Expression>, Option<Box<Expression>>) },
    Lambda { formals: (Vec<Expression>, Option<Box<Expression>>), body: Vec<Expression> },
    Conditional { test: Box<Expression>, consequent: Box<Expression>, alternate: Option<Box<Expression>> },
    Assignment { id: Box<Expression>, value: Box<Expression> },
    Definition { formals: (Vec<Expression>, Option<Box<Expression>>), body: Vec<Expression> },
    Block { body: Vec<Expression> },
}
#[derive(Debug, Clone)]
pub enum Literal {
    Nil,
    Boolean(bool),
    Integer(i64),
    Rational(i64, i64),
    Float(f64),
    Character(char),
    String(String),
    Bytes(Vec<u8>),
    Symbol(String),
    StaticSymbol(&'static str),
    List(Vec<Literal>, Option<Box<Literal>>),
    Vector(Vec<Literal>),
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
        println!("final form: {:#?}", se);
    }
}
