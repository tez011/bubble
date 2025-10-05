use crate::syntax;
use crate::syntax::{LiteralC, LiteralD};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueID(pub usize);
impl From<&syntax::Expression> for ValueID {
    fn from(value: &syntax::Expression) -> Self {
        if let syntax::Expression::Variable(i) = value {
            ValueID(*i)
        } else {
            panic!()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContinuationID(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Continuation {
    Escape,
    Abort,
    To(ContinuationID),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Atom {
    Literal(LiteralC),
    Variable(ValueID),
    CorePrimitive(&'static str),
}

#[derive(Debug, Clone)]
pub struct Lambda {
    /// The defined parameters of this function.
    pub(crate) formals: (Vec<ValueID>, Option<ValueID>),
    /// The continuation to invoke with the return value.
    pub(crate) k: Continuation,
    pub(crate) body: Box<Expression>,
}

#[derive(Debug, Clone)]
pub enum Expression {
    /// Identical to `body`, but with the literal `val` bound to the variable index `var`.
    /// `pure` if the only use of `val` is within `body`.
    LetLiteral { id: ValueID, val: LiteralD, body: Box<Expression> },
    /// Identical to `body`, but with the lambda `val` bound to the variable index `var`.
    /// `pure` if the only use of `val` is within `body`.
    LetLambda { id: ValueID, val: Lambda, body: Box<Expression> },
    /// Identical to `body`, but with the continuation `cont` bound to the variable index `var`.
    LetContinuation { id: ContinuationID, kf: (Vec<ValueID>, Option<ValueID>), k: Box<Expression>, body: Box<Expression> },
    /// Calls the lambda or primitive identified by `operator` with parameters identified by `operands`.
    /// Invokes the continuation `k` with the appropriate return values upon success.
    Apply { operator: Atom, operands: (Vec<Atom>, Option<Atom>), k: Continuation },
    /// Identical to `consequent` unless `test` evaluates to false; then, `alternate`.
    Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
    /// Invokes `k` with `values`.
    Continue { k: Continuation, values: (Vec<Atom>, Option<Atom>) },
    /// Invokes `k` with no values after binding elements of `val`, in order, to elements of `var`, any leftovers in the tail.
    Assign { vars: (Vec<ValueID>, Option<ValueID>), values: (Vec<Atom>, Option<Atom>), k: Continuation },
}

impl Expression {
    /// Atomizes an expression `e` so it can be used in other expressions.
    /// If the expression is not atomic, its value is bound to a new variable.
    /// Returns an expression that is the result of using the atom (with `then`), whether directly,
    /// or after binding it to a variable.
    fn atomize(e: syntax::Expression, env: &syntax::Environment, then: impl FnOnce(Atom) -> Expression) -> Self {
        use syntax::Expression as stx;
        match e {
            stx::CorePrimitive(s) => then(Atom::CorePrimitive(s)),
            stx::Variable(i) => then(Atom::Variable(ValueID(i))),
            stx::Literal(syntax::Literal::Copy(e)) => then(Atom::Literal(e)),
            _ => {
                let x = ValueID(env.new_variable());
                let k = ContinuationID(env.new_variable());
                Self::LetContinuation {
                    id: k,
                    kf: (vec![x], None),
                    k: Box::new(then(Atom::Variable(x))),
                    body: Box::new(Self::new(e, Continuation::To(k), env)),
                }
            }
        }
    }

    /// Atomizes a bunch of expressions `ee`, just like `atomize`. `then` is invoked with all the atoms in `ee`.
    fn atomize_many(ee: Vec<syntax::Expression>, env: &syntax::Environment, then: impl FnOnce(Vec<Atom>) -> Expression) -> Self {
        use syntax::Expression as stx;
        let (atoms, new_ids) = ee.iter().map(|e| match e {
            stx::CorePrimitive(s) => (Atom::CorePrimitive(s), None),
            stx::Variable(i) => (Atom::Variable(ValueID(*i)), None),
            stx::Literal(syntax::Literal::Copy(e)) => (Atom::Literal(*e), None),
            _ => {
                let x = ValueID(env.new_variable());
                (Atom::Variable(x), Some(x))
            },
        }).collect::<(Vec<_>, Vec<_>)>();
        std::iter::zip(new_ids.into_iter(), ee.into_iter()).rev()
            .fold(then(atoms), |acc, (id, e)| match id {
                None => acc,
                Some(x) => {
                    let k = ContinuationID(env.new_variable());
                    Self::LetContinuation {
                        id: k,
                        kf: (vec![x], None),
                        k: Box::new(acc),
                        body: Box::new(Self::new(e, Continuation::To(k), env)),
                    }
                }
            })
    }

    /// Transforms `e` into continuation-passing style and invokes `k` on its result.
    fn new(e: syntax::Expression, k: Continuation, env: &syntax::Environment) -> Self {
        use syntax::Expression as stx;
        use Expression::*;

        match e {
            stx::CorePrimitive(s) => Continue { k, values: (vec![Atom::CorePrimitive(s)], None) },
            stx::Variable(i) => Continue { k, values: (vec![Atom::Variable(ValueID(i))], None) },
            stx::Literal(syntax::Literal::Copy(val)) => Continue { k, values: (vec![Atom::Literal(val)], None) },
            stx::Literal(syntax::Literal::Clone(val)) => {
                let var = ValueID(env.new_variable());
                LetLiteral {
                    id: var,
                    val,
                    body: Box::new(Continue { k, values: (vec![Atom::Variable(var)], None) }),
                }
            },
            stx::ProcedureCall { operator, operands } => {
                Self::atomize(*operator, env, move |operator|
                    Self::atomize_many(operands, env, move |operands|
                        Self::new_application(operator, operands, k, env)))
            },
            stx::Lambda { formals, body } => {
                let x = ValueID(env.new_variable());
                let kx = ContinuationID(env.new_variable());
                LetLambda {
                    id: x,
                    val: Lambda {
                        formals: (formals.0.iter().map(ValueID::from).collect(), formals.1.map(|e| ValueID::from(e.as_ref()))),
                        k: Continuation::To(kx),
                        body: Box::new(Self::new_block(body, Continuation::To(kx), env)),
                    },
                    body: Box::new(Continue {
                        k,
                        values: (vec![Atom::Variable(x)], None),
                    }),
                }
            },
            stx::Conditional { test, consequent, alternate } => Self::atomize(*test, env, move |test| Branch {
                test,
                consequent: Box::new(Expression::new(*consequent, k, env)),
                alternate: Box::new(alternate.map(|e| Expression::new(*e, k, env))
                    .unwrap_or_else(|| Continue {
                        k,
                        values: (vec![Atom::Literal(LiteralC::Boolean(false))], None)
                    })),
            }),
            stx::Assignment { ids, value } | stx::Definition { ids, value } => {
                Self::atomize(*value, env, |value| Assign {
                    vars: (ids.0.iter().map(ValueID::from).collect(), ids.1.map(|e| ValueID::from(e.as_ref()))),
                    values: (vec![], Some(value)),
                    k,
                })
            },
            stx::Block { body } => Self::new_block(body, k, env),
        }
    }
    fn new_application(operator: Atom, operands: Vec<Atom>, k: Continuation, env: &syntax::Environment) -> Self {
        use Expression::*;
        match operator {
            Atom::CorePrimitive("apply") => {
                let proc = ValueID(env.new_variable());
                let args = ValueID(env.new_variable());
                let k0 = ContinuationID(env.new_variable());
                LetContinuation {
                    id: k0,
                    kf: (vec![proc], Some(args)),
                    k: Box::new(Apply {
                        operator: Atom::Variable(proc),
                        operands: (vec![], Some(Atom::Variable(args))),
                        k,
                    }),
                    body: Box::new(Continue { k: Continuation::To(k0), values: (operands, None) }),
                }
            },
            Atom::CorePrimitive("call/cc" | "call-with-current-continuation") => {
                let dynamic_extent = ValueID(env.new_variable());
                let proc = ValueID(env.new_variable());
                let args = ValueID(env.new_variable());
                let reified_cont = ValueID(env.new_variable());
                let k0 = ContinuationID(env.new_variable());
                let k1 = ContinuationID(env.new_variable());
                let k2 = ContinuationID(env.new_variable());
                LetContinuation {
                    id: k0,
                    kf: (vec![dynamic_extent], None),
                    k: Box::new(LetContinuation {
                        id: k1,
                        kf: (vec![proc], None),
                        k: Box::new(LetLambda {
                            id: reified_cont,
                            val: Lambda {
                                formals: (vec![], Some(args)),
                                k: Continuation::Escape,
                                body: Box::new(LetContinuation {
                                    id: k2,
                                    kf: (vec![], None),
                                    k: Box::new(Continue { k, values: (vec![], Some(Atom::Variable(args))) }),
                                    body: Box::new(Apply {
                                        operator: Atom::CorePrimitive("__rewind_dynamic_extent"),
                                        operands: (vec![Atom::Variable(dynamic_extent)], None),
                                        k: Continuation::To(k2),
                                    }),
                                }),
                            },
                            body: Box::new(Apply {
                                operator: Atom::Variable(proc),
                                operands: (vec![Atom::Variable(reified_cont)], None),
                                k,
                            }),
                        }),
                        body: Box::new(Continue { k: Continuation::To(k1), values: (operands, None) }),
                    }),
                    body: Box::new(Apply {
                        operator: Atom::CorePrimitive("__store_dynamic_extent"),
                        operands: (vec![], None),
                        k: Continuation::To(k0),
                    }),
                }
            },
            Atom::CorePrimitive("call-with-values") => {
                let producer = ValueID(env.new_variable());
                let consumer = ValueID(env.new_variable());
                let the_values = ValueID(env.new_variable());
                let k0 = ContinuationID(env.new_variable());
                let k1 = ContinuationID(env.new_variable());
                LetContinuation {
                    id: k0,
                    kf: (vec![producer, consumer], None),
                    k: Box::new(LetContinuation {
                        id: k1,
                        kf: (vec![], Some(the_values)),
                        k: Box::new(Apply {
                            operator: Atom::Variable(consumer),
                            operands: (vec![], Some(Atom::Variable(the_values))),
                            k,
                        }),
                        body: Box::new(Apply {
                            operator: Atom::Variable(producer),
                            operands: (vec![], None),
                            k: Continuation::To(k1),
                        }),
                    }),
                    body: Box::new(Continue { k: Continuation::To(k0), values: (operands, None) }),
                }
            },
            Atom::CorePrimitive("dynamic-wind") => {
                let dynamic_extent = ValueID(env.new_variable());
                let before = ValueID(env.new_variable());
                let thunk = ValueID(env.new_variable());
                let after = ValueID(env.new_variable());
                let k0 = ContinuationID(env.new_variable());
                let k1 = ContinuationID(env.new_variable());
                let k2 = ContinuationID(env.new_variable());
                let k3 = ContinuationID(env.new_variable());
                LetContinuation {
                    id: k0,
                    kf: (vec![before, thunk, after], None),
                    k: Box::new(LetContinuation {
                        id: k1,
                        kf: (vec![], None),
                        k: Box::new(LetContinuation {
                            id: k2,
                            kf: (vec![dynamic_extent], None),
                            k: Box::new(LetContinuation {
                                id: k3,
                                kf: (vec![], None),
                                k: Box::new(Apply {
                                    operator: Atom::CorePrimitive("__rewind_dynamic_extent"),
                                    operands: (vec![Atom::Variable(dynamic_extent)], None),
                                    k,
                                }),
                                body: Box::new(Apply {
                                    operator: Atom::Variable(thunk),
                                    operands: (vec![], None),
                                    k: Continuation::To(k3),
                                }),
                            }),
                            body: Box::new(Apply {
                                operator: Atom::CorePrimitive("__push_dynamic_frame"),
                                operands: (vec![Atom::Variable(before), Atom::Variable(after)], None),
                                k: Continuation::To(k2),
                            }),
                        }),
                        body: Box::new(Apply {
                            operator: Atom::Variable(before),
                            operands: (vec![], None),
                            k: Continuation::To(k1),
                        }),
                    }),
                    body: Box::new(Continue { k: Continuation::To(k0), values: (operands, None) }),
                }
            },
            Atom::CorePrimitive("values") => Continue { k, values: (operands, None) },
            Atom::CorePrimitive("raise") => {
                let eh = ValueID(env.new_variable());
                let old_extent = ValueID(env.new_variable());
                let k0 = ContinuationID(env.new_variable());
                let k1 = ContinuationID(env.new_variable());
                LetContinuation {
                    id: k0,
                    kf: (vec![eh, old_extent], None),
                    k: Box::new(LetContinuation {
                        id: k1,
                        kf: (vec![], None),
                        k: Box::new(Apply {
                            operator: Atom::Variable(eh),
                            operands: (operands, None),
                            k: Continuation::Abort,
                        }),
                        body: Box::new(Apply {
                            operator: Atom::CorePrimitive("__rewind_dynamic_extent"),
                            operands: (vec![Atom::Variable(old_extent)], None),
                            k: Continuation::To(k1),
                        })
                    }),
                    body: Box::new(Apply {
                        operator: Atom::CorePrimitive("__find_exception_handler"),
                        operands: (vec![], None),
                        k: Continuation::To(k0),
                    }),
                }
            },
            Atom::CorePrimitive("raise-continuable") => {
                let eh = ValueID(env.new_variable());
                let k0 = ContinuationID(env.new_variable());
                LetContinuation {
                    id: k0,
                    kf: (vec![eh], None), // this silently trims the arity
                    k: Box::new(Apply {
                        operator: Atom::Variable(eh),
                        operands: (operands, None),
                        k,
                    }),
                    body: Box::new(Apply {
                        operator: Atom::CorePrimitive("__find_exception_handler"),
                        operands: (vec![], None),
                        k: Continuation::To(k0),
                    }),
                }
            },
            Atom::CorePrimitive("with-exception-handler") => {
                let dynamic_extent = ValueID(env.new_variable());
                let handler = ValueID(env.new_variable());
                let thunk = ValueID(env.new_variable());
                let k0 = ContinuationID(env.new_variable());
                let k1 = ContinuationID(env.new_variable());
                let k2 = ContinuationID(env.new_variable());
                LetContinuation {
                    id: k0,
                    kf: (vec![handler, thunk], None),
                    k: Box::new(LetContinuation {
                        id: k1,
                        kf: (vec![dynamic_extent], None),
                        k: Box::new(LetContinuation {
                            id: k2,
                            kf: (vec![], None),
                            k: Box::new(Apply {
                                operator: Atom::CorePrimitive("__rewind_dynamic_extent"),
                                operands: (vec![Atom::Variable(dynamic_extent)], None),
                                k,
                            }),
                            body: Box::new(Apply {
                                operator: Atom::Variable(thunk),
                                operands: (vec![], None),
                                k: Continuation::To(k2),
                            }),
                        }),
                        body: Box::new(Apply {
                            operator: Atom::CorePrimitive("__push_exception_handler"),
                            operands: (vec![Atom::Variable(handler)], None),
                            k: Continuation::To(k1),
                        }),
                    }),
                    body: Box::new(Continue { k: Continuation::To(k0), values: (operands, None) }),
                }
            },
            Atom::CorePrimitive("make-parameter") => {
                let init = ValueID(env.new_variable());
                let parameter = ValueID(env.new_variable());
                let present = ValueID(env.new_variable());
                let value = ValueID(env.new_variable());
                let k0 = ContinuationID(env.new_variable());
                let k1 = ContinuationID(env.new_variable());
                let bound_k = ContinuationID(env.new_variable());
                LetContinuation {
                    id: k0,
                    kf: (vec![init], None),
                    k: Box::new(LetLambda {
                        id: parameter,
                        val: Lambda {
                            formals: (vec![], None),
                            k: Continuation::To(bound_k),
                            body: Box::new(LetContinuation {
                                id: k1,
                                kf: (vec![present, value], None),
                                k: Box::new(Branch {
                                    test: Atom::Variable(present),
                                    consequent: Box::new(Continue {
                                        k: Continuation::To(bound_k),
                                        values: (vec![Atom::Variable(value)], None),
                                    }),
                                    alternate: Box::new(Continue {
                                        k: Continuation::To(bound_k),
                                        values: (vec![Atom::Variable(init)], None),
                                    }),
                                }),
                                body: Box::new(Apply {
                                    operator: Atom::CorePrimitive("__find_parameter_frame"),
                                    operands: (vec![Atom::Variable(parameter)], None),
                                    k: Continuation::To(k1),
                                }),
                            }),
                        },
                        body: Box::new(Continue {
                            k,
                            values: (vec![Atom::Variable(parameter)], None)
                        }),
                    }),
                    body: Box::new(Continue { k: Continuation::To(k0), values: (operands, None) }),
                }
            },
            Atom::CorePrimitive("let-parameter") => {
                let parameter = ValueID(env.new_variable());
                let value = ValueID(env.new_variable());
                let body_thunk = ValueID(env.new_variable());
                let dynamic_extent = ValueID(env.new_variable());
                let body_value = ValueID(env.new_variable());
                let k0 = ContinuationID(env.new_variable());
                let k1 = ContinuationID(env.new_variable());
                let k2 = ContinuationID(env.new_variable());
                let k3 = ContinuationID(env.new_variable());
                LetContinuation {
                    id: k0,
                    kf: (vec![parameter, value, body_thunk], None),
                    k: Box::new(LetContinuation {
                        id: k1,
                        kf: (vec![dynamic_extent], None),
                        k: Box::new(LetContinuation {
                            id: k2,
                            kf: (vec![], Some(body_value)),
                            k: Box::new(LetContinuation {
                                id: k3,
                                kf: (vec![], None),
                                k: Box::new(Continue { k, values: (vec![], Some(Atom::Variable(body_value))) }),
                                body: Box::new(Apply {
                                    operator: Atom::CorePrimitive("__rewind_dynamic_extent"),
                                    operands: (vec![Atom::Variable(dynamic_extent)], None),
                                    k: Continuation::To(k3),
                                }),
                            }),
                            body: Box::new(Apply {
                                operator: Atom::Variable(body_thunk),
                                operands: (vec![], None),
                                k: Continuation::To(k2),
                            }),
                        }),
                        body: Box::new(Apply {
                            operator: Atom::CorePrimitive("__push_parameter_frame"),
                            operands: (vec![Atom::Variable(parameter), Atom::Variable(value)], None),
                            k: Continuation::To(k1),
                        }),
                    }),
                    body: Box::new(Continue { k: Continuation::To(k0), values: (operands, None) }),
                }
            },
            _ => Apply { operator, operands: (operands, None), k },
        }
    }
    fn new_block(e: Vec<syntax::Expression>, k: Continuation, env: &syntax::Environment) -> Self {
        use Expression::*;

        let mut ee = e.into_iter().rev();
        if let Some(tail) = ee.next() {
            ee.fold(Self::new(tail, k, env), |acc, e| {
                let kx = ContinuationID(env.new_variable());
                LetContinuation {
                    id: kx,
                    kf: (vec![], None),
                    k: Box::new(acc),
                    body: Box::new(Self::new(e, Continuation::To(kx), env)),
                }
            })
        } else {
            Continue { k, values: (vec![], None) }
        }
    }
}

pub fn transform(e: syntax::Expression, k: Continuation, env: &syntax::Environment) -> Expression {
    Expression::new(e, k, env)
}
