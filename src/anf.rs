use crate::cps;
use crate::syntax;
use crate::syntax::Literal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueID(pub usize);
impl From<cps::ValueID> for ValueID {
    fn from(value: cps::ValueID) -> Self {
        Self(value.0)
    }
}
impl From<cps::ContinuationID> for ValueID {
    fn from(value: cps::ContinuationID) -> Self {
        Self(value.0)
    }
}

#[derive(Debug, Clone)]
pub enum Atom {
    Literal(syntax::Literal),
    Variable(ValueID),
    Lambda(Lambda),
    CorePrimitive(&'static str),
}
impl From<cps::Atom> for Atom {
    fn from(value: cps::Atom) -> Self {
        match value {
            cps::Atom::Literal(lit) => Atom::Literal(Literal::Copy(lit)),
            cps::Atom::Variable(id) => Atom::Variable(id.into()),
            cps::Atom::CorePrimitive(s) => Atom::CorePrimitive(s),
        }
    }
}

#[derive(Debug)]
struct Environment {
    current_return: std::cell::Cell<cps::Continuation>,
}
impl Default for Environment {
    fn default() -> Self {
        Self { current_return: std::cell::Cell::new(cps::Continuation::Escape) }
    }
}
impl Environment {
    fn with_return<R>(&self, k: cps::Continuation, f: impl FnOnce(&Self) -> R) -> R {
        let old = self.current_return.get();
        self.current_return.set(k);
        let r = f(self);
        self.current_return.set(old);
        r
    }
}

#[derive(Debug, Clone)]
pub struct Lambda {
    /// The defined parameters of this function.
    formals: (Vec<ValueID>, Option<ValueID>),
    body: Box<Expression>,
}
impl Lambda {
    fn new(val: cps::Lambda, env: &Environment) -> Self {
        Self {
            formals: (val.formals.0.iter().copied().map(ValueID::from).collect(), val.formals.1.map(ValueID::from)),
            body: Box::new(Expression::from_cps(*val.body, env)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Atom(Atom),
    Application { operator: Atom, operands: (Vec<Atom>, Option<Atom>) },
}

#[derive(Debug, Clone, Default)]
pub enum Expression {
    #[default] Nop,
    Let { id: ValueID, val: Value, body: Box<Expression> },
    Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
    Assign { vars: (Vec<ValueID>, Option<ValueID>), values: (Vec<Value>, Option<Value>), body: Box<Expression> },
    Return(Vec<Value>, Option<Value>),
    TailCall { to: Atom, values: (Vec<Value>, Option<Value>) },
    Halt,
}
impl Expression {
    fn from_cps(e: cps::Expression, env: &Environment) -> Self {
        match e {
            cps::Expression::LetLiteral { id, val, body, .. } => Self::Let {
                id: id.into(),
                val: Value::Atom(Atom::Literal(Literal::Clone(val))),
                body: Box::new(Self::from_cps(*body, env)),
            },
            cps::Expression::LetLambda { id, val, body, .. } => env.with_return(val.k, |env| Self::Let {
                id: id.into(),
                val: Value::Atom(Atom::Lambda(Lambda::new(val, env))),
                body: Box::new(Self::from_cps(*body, env)),
            }),
            cps::Expression::LetContinuation { id, kf, k, body } => Self::Let {
                id: id.into(),
                val: Value::Atom(Atom::Lambda(Lambda {
                    formals: (kf.0.iter().copied().map(ValueID::from).collect(), kf.1.map(ValueID::from)),
                    body: Box::new(Self::from_cps(*k, env)),
                })),
                body: Box::new(Self::from_cps(*body, env)),
            },
            cps::Expression::Apply { operator, operands, k } => if k == env.current_return.get() || k == cps::Continuation::Escape {
                Self::Return(vec![], Some(Value::Application {
                    operator: Atom::from(operator),
                    operands: (operands.0.iter().copied().map(Atom::from).collect(), operands.1.map(Atom::from)),
                }))
            } else if let cps::Continuation::To(k) = k {
                Self::TailCall { to: Atom::Variable(k.into()), values: (vec![], Some(Value::Application {
                    operator: Atom::from(operator),
                    operands: (operands.0.iter().copied().map(Atom::from).collect(), operands.1.map(Atom::from)),
                })) }
            } else {
                Self::Halt
            }
            cps::Expression::Branch { test, consequent, alternate } => Self::Branch {
                test: Atom::from(test),
                consequent: Box::new(Expression::from_cps(*consequent, env)),
                alternate: Box::new(Expression::from_cps(*alternate, env)),
            },
            cps::Expression::Continue { k, values } => if k == env.current_return.get() || k == cps::Continuation::Escape {
                Self::Return(values.0.iter().copied().map(Atom::from).map(Value::Atom).collect(), values.1.map(Atom::from).map(Value::Atom))
            } else if let cps::Continuation::To(k) = k {
                Self::TailCall {
                    to: Atom::Variable(k.into()),
                    values: (values.0.iter().copied().map(Atom::from).map(Value::Atom).collect(), values.1.map(Atom::from).map(Value::Atom)),
                }
            } else {
                Self::Halt
            }
            cps::Expression::Assign { vars, values, k } => Self::Assign {
                vars: (vars.0.iter().copied().map(ValueID::from).collect(), vars.1.map(ValueID::from)),
                values: (values.0.iter().copied().map(Atom::from).map(Value::Atom).collect(), values.1.map(Atom::from).map(Value::Atom)),
                body: Box::new(if k == env.current_return.get() || k == cps::Continuation::Escape {
                    Self::Return(vec![], None)
                } else if let cps::Continuation::To(k) = k {
                    Self::TailCall { to: Atom::Variable(k.into()), values: (vec![], None) }
                } else {
                    Self::Halt
                }),
            },
        }
    }
}

pub fn transform(e: syntax::Expression, stx_env: &syntax::Environment) -> Expression {
    let anf_env = Default::default();
    let e = cps::transform(e, cps::Continuation::Escape, stx_env);
    let e = Expression::from_cps(e, &anf_env);
    e
}

