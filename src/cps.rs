use std::vec;

use crate::syntax;
use crate::syntax::{LiteralC, LiteralD};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] pub struct ValueID(pub usize);
impl From<&syntax::Expression> for ValueID {
    fn from(e: &syntax::Expression) -> Self {
        match e {
            syntax::Expression::Variable(i) => ValueID(*i),
            _ => panic!(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] pub struct ContinuationID(pub usize);

#[derive(Debug, Clone, Copy)]
pub enum Atom {
    Literal(LiteralC),
    Variable(ValueID),
    Continuation(ContinuationID),
    CorePrimitive(&'static str),
}

#[derive(Debug, Clone)]
pub struct Lambda {
    /// The defined parameters of this function.
    formals: (Vec<ValueID>, Option<ValueID>),
    /// An additional parameter: the continuation to invoke with the return value.
    k: ContinuationID,
    /// An additional parameter: the continuation to invoke with an exception.
    kf: ContinuationID,
    body: Box<Expression>,
}

#[derive(Debug, Clone)]
pub enum Continuation {
    Variable(ContinuationID),
    Implicit { formals: (Vec<ValueID>, Option<ValueID>), body: Box<Expression> },
}

#[derive(Debug, Clone)]
pub enum Expression {
    /// Identical to `body`, but with the literal `val` bound to the variable index `var`.
    LetLiteral { var: ValueID, val: LiteralD, body: Box<Expression> },
    /// Identical to `body`, but with the lambda `val` bound to the variable index `var`.
    LetLambda { var: ValueID, val: Lambda, body: Box<Expression>, },
    /// Identical to `body`, but with the continuation `cont` bound to the variable index `var`.
    LetContinuation { var: ContinuationID, cont: Continuation, body: Box<Expression> },
    /// Calls the lambda or primitive identified by `operator` with parameters identified by `operands`.
    /// Invokes the continuation `k` with the appropriate return values upon success.
    /// Invokes the continuation `kf` on an exception.
    Apply { operator: Atom, operands: (Vec<Atom>, Option<Atom>), k: Continuation, kf: Continuation },
    /// Identical to `consequent` unless `test` evaluates to false; then, `alternate`.
    Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
    /// Invokes `k` with `values`.
    Continue { k: Continuation, values: (Vec<Atom>, Option<Atom>) },
    /// Invokes `k` with no values after binding `var` to `val`.
    Assign { var: ValueID, val: Atom, k: Continuation },
    /// Represents finality in execution. Does not continue.
    Halt { values: (Vec<Atom>, Option<Atom>) },
    /// Represents fatality in execution. Does not continue.
    Abort { values: (Vec<Atom>, Option<Atom>) },
}

impl crate::Environment {
    /// Atomizes an expression `e` so it can be used in other expressions, with `kf` the failure continuation.
    /// If the expression is not atomic, its value is bound to a new variable.
    /// Returns an expression that is the result of using the atom (with `then`), whether directly, or after binding it to a variable.
    fn atomize(&self, e: syntax::Expression, kf: ContinuationID, then: impl FnOnce(Atom) -> Expression) -> Expression {
        match e {
            syntax::Expression::CorePrimitive(s) => then(Atom::CorePrimitive(s)),
            syntax::Expression::Variable(i) => then(Atom::Variable(ValueID(i))),
            syntax::Expression::Literal(syntax::Literal::Copy(e)) => then(Atom::Literal(e)),
            _ => {
                let x = ValueID(self.new_variable());
                let k = ContinuationID(self.new_variable());
                Expression::LetContinuation {
                    var: k,
                    cont: Continuation::Implicit { formals: (vec![x], None), body: Box::new(then(Atom::Variable(x))) },
                    body: Box::new(self.transform(e, k, kf)),
                }
            }
        }
    }

    /// Atomizes a bunch of expressions `ee`, just like `atomize`. `then` is invoked with all the atoms in `ee`.
    fn atomize_many(&self, ee: Vec<syntax::Expression>, kf: ContinuationID, then: impl FnOnce(Vec<Atom>) -> Expression) -> Expression {
        let (atoms, new_ids) = ee.iter().map(|e| match e {
            syntax::Expression::CorePrimitive(s) => (Atom::CorePrimitive(s), None),
            syntax::Expression::Variable(i) => (Atom::Variable(ValueID(*i)), None),
            syntax::Expression::Literal(syntax::Literal::Copy(e)) => (Atom::Literal(*e), None),
            _ => {
                let x = ValueID(self.new_variable());
                (Atom::Variable(x), Some(x))
            },
        }).collect::<(Vec<_>, Vec<_>)>();
        std::iter::zip(new_ids.into_iter(), ee.into_iter()).rev()
            .fold(then(atoms), |acc, (id, e)| match id {
                None => acc,
                Some(id) => {
                    let kx = ContinuationID(self.new_variable());
                    Expression::LetContinuation {
                        var: kx,
                        cont: Continuation::Implicit { formals: (vec![id], None), body: Box::new(acc), },
                        body: Box::new(self.transform(e, kx, kf)),
                    }
                }
            })
    }

    /// Transforms `e` into continuation-passing style and invokes `k` on it.
    /// Exceptions are handled by calling `kf`.
    fn transform(&self, e: syntax::Expression, k: ContinuationID, kf: ContinuationID) -> Expression {
        match e {
            syntax::Expression::CorePrimitive(s) => Expression::Continue {
                k: Continuation::Variable(k),
                values: (vec![Atom::CorePrimitive(s)], None) },
            syntax::Expression::Variable(i) => Expression::Continue {
                k: Continuation::Variable(k),
                values: (vec![Atom::Variable(ValueID(i))], None) },
            syntax::Expression::Literal(syntax::Literal::Copy(e)) => Expression::Continue {
                k: Continuation::Variable(k),
                values: (vec![Atom::Literal(e)], None) },
            syntax::Expression::Literal(syntax::Literal::Clone(e)) => {
                let x = ValueID(self.new_variable());
                Expression::LetLiteral {
                    var: x,
                    val: e,
                    body: Box::new(Expression::Continue {
                        k: Continuation::Variable(k),
                        values: (vec![Atom::Variable(x)], None),
                    }),
                } },
            syntax::Expression::ProcedureCall { operator, operands } => {
                self.atomize(*operator, kf, move |operator|
                    self.atomize_many(operands.0, kf, move |operands_0| match operands.1 {
                        None => Expression::Apply { operator,
                            operands: (operands_0, None),
                            k: Continuation::Variable(k),
                            kf: Continuation::Variable(kf) },
                        Some(operands_1) => self.atomize(*operands_1, kf, move |operands_1| Expression::Apply { operator,
                            operands: (operands_0, Some(operands_1)),
                            k: Continuation::Variable(k),
                            kf: Continuation::Variable(kf) }),
                    }))
            }
            syntax::Expression::Lambda { formals, body } => {
                let x = ValueID(self.new_variable());
                let kx = ContinuationID(self.new_variable());
                let kfx = ContinuationID(self.new_variable());
                Expression::LetLambda {
                    var: x,
                    val: Lambda {
                        formals: (formals.0.iter().map(|e| ValueID::from(e)).collect(), formals.1.map(|e| ValueID::from(e.as_ref()))),
                        k: kx,
                        kf: kfx,
                        body: Box::new(self.transform_block(body, kx, kfx)),
                    },
                    body: Box::new(Expression::Continue {
                        k: Continuation::Variable(k),
                        values: (vec![Atom::Variable(x)], None),
                    }),
                }
            }
            syntax::Expression::Conditional { test, consequent, alternate } => {
                self.atomize(*test, kf, move |test| match test {
                    // optimization: evaluate the branch for atomic values of test
                    Atom::Literal(LiteralC::Boolean(false)) => match alternate {
                        None => Expression::Continue {
                            k: Continuation::Variable(k),
                            values: (vec![Atom::Literal(LiteralC::Boolean(false))], None),
                        },
                        Some(alternate) => self.transform(*alternate, k, kf),
                    },
                    Atom::CorePrimitive(_) | Atom::Literal(_) | Atom::Continuation(_) => self.transform(*consequent, k, kf),
                    Atom::Variable(_) => Expression::Branch {
                        test,
                        consequent: Box::new(self.transform(*consequent, k, kf)),
                        alternate: Box::new(match alternate {
                            None => Expression::Continue {
                                k: Continuation::Variable(k),
                                values: (vec![Atom::Literal(LiteralC::Boolean(false))], None),
                            },
                            Some(alternate) => self.transform(*alternate, k, kf),
                        }),
                    },
                })
            }
            syntax::Expression::Assignment { id, value } => {
                self.atomize(*value, kf, move |val| Expression::Assign {
                    var: ValueID::from(id.as_ref()),
                    val,
                    k: Continuation::Variable(k),
                })
            },
            syntax::Expression::Definition { formals, body } => {
                let kx = ContinuationID(self.new_variable());
                Expression::LetContinuation {
                    var: kx,
                    cont: Continuation::Implicit {
                        formals: (formals.0.iter().map(|e| ValueID::from(e)).collect(), formals.1.map(|e| ValueID::from(e.as_ref()))),
                        body: Box::new(Expression::Continue { k: Continuation::Variable(k), values: (vec![], None) }),
                    },
                    body: Box::new(self.transform(*body, kx, kf)),
                }
            }
            syntax::Expression::Block { body } => self.transform_block(body, k, kf),
        }
    }
    fn transform_block(&self, e: Vec<syntax::Expression>, k: ContinuationID, kf: ContinuationID) -> Expression {
        let mut ee = e.into_iter().rev();
        match ee.next() {
            None => Expression::Continue { k: Continuation::Variable(k), values: (vec![], None) },
            Some(tail) => ee.fold(self.transform(tail, k, kf), |acc, e| {
                    let kx = ContinuationID(self.new_variable());
                    Expression::LetContinuation {
                        var: kx,
                        cont: Continuation::Implicit {
                            formals: (vec![], Some(ValueID(self.new_variable()))),
                            body: Box::new(acc),
                        },
                        body: Box::new(self.transform(e, kx, kf)),
                    }
                }),
        }
    }
}

pub fn transform(e: syntax::Expression, k: ContinuationID, kf: ContinuationID, env: &crate::Environment) -> Expression {
    env.transform(e, k, kf)
}
