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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Default)]
pub enum Continuation {
    #[default] OptimizedOut,
    Variable(ContinuationID),
    Implicit { formals: (Vec<ValueID>, Option<ValueID>), body: Box<Expression> },
}

#[derive(Debug, Clone, Default)]
pub enum Expression {
    /// Identical to `body`, but with the literal `val` bound to the variable index `var`.
    LetLiteral { var: ValueID, val: LiteralD, body: Box<Expression> },
    /// Identical to `body`, but with the lambda `val` bound to the variable index `var`.
    LetLambda { var: ValueID, val: Lambda, body: Box<Expression>, },
    /// Identical to `body`, but with the continuation `cont` bound to the variable index `var`.
    LetContinuation { var: ContinuationID, k: Continuation, body: Box<Expression> },
    /// Calls the lambda or primitive identified by `operator` with parameters identified by `operands`.
    /// Invokes the continuation `k` with the appropriate return values upon success.
    /// Invokes the continuation `kf` on an exception.
    Apply { operator: Atom, operands: (Vec<Atom>, Option<Atom>), k: ContinuationID, kf: ContinuationID },
    /// Identical to `consequent` unless `test` evaluates to false; then, `alternate`.
    Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
    /// Invokes `k` with `values`.
    Continue { k: Continuation, values: (Vec<Atom>, Option<Atom>) },
    /// Invokes `k` with no values after binding elements of `val`, in order, to elements of `var`, any leftovers in the tail.
    Assign { vars: (Vec<ValueID>, Option<ValueID>), values: (Vec<Atom>, Option<Atom>), k: Continuation },
    /// Represents finality in execution. Does not continue.
    Halt { values: Vec<Atom> },
    /// Represents fatality in execution. Does not continue.
    Abort { values: Vec<Atom> },
    /// Represents finality in execution. Does not continue.
    #[default] HaltEmpty,
}

impl Lambda {
    fn uses_variable(&self, v: ValueID) -> bool {
        self.formals.0.iter().chain(self.formals.1.iter()).any(|i| *i == v) || self.body.uses_variable(v)
    }
    fn uses_continuation(&self, v: ContinuationID) -> bool {
        self.k == v || self.kf == v || self.body.uses_continuation(v)
    }
}

impl Continuation {
    fn rename(self, from: &(Vec<ValueID>, Option<ValueID>), to: &(Vec<Atom>, Option<Atom>)) -> Self {
        match self {
            Continuation::Implicit { formals, body } => Continuation::Implicit {
                formals,
                body: Box::new(body.rename(from, to)),
            },
            _ => self,
        }
    }
    fn rename_continues(self, from: &[ContinuationID], to: &[ContinuationID]) -> Self {
        let mapping = std::iter::zip(from.iter().copied(), to.iter().copied()).collect::<std::collections::HashMap<_, _>>();
        match self {
            Continuation::OptimizedOut => self,
            Continuation::Variable(i) => Continuation::Variable(mapping.get(&i).copied().unwrap_or(i)),
            Continuation::Implicit { formals, body } => Continuation::Implicit { formals, body: Box::new(body.rename_continues(from, to)) },
        }
    }

    fn uses_variable(&self, v: ValueID) -> bool {
        if let Self::Implicit { formals, body } = self {
            formals.0.iter().chain(formals.1.iter()).any(|i| *i == v) || body.uses_variable(v)
        } else {
            false
        }
    }
    fn uses_continuation(&self, v: ContinuationID) -> bool {
        match self {
            Continuation::OptimizedOut => false,
            Continuation::Variable(i) => *i == v,
            Continuation::Implicit { body, .. } => body.uses_continuation(v),
        }
    }
}

impl Expression {
    fn visit_multi<T>(&mut self, mut accumulator: T, mut f: impl FnMut(&mut Self, &mut T) -> bool) -> (T, bool) {
        fn visit_children<T>(expr: &mut Expression, a: &mut T, f: &mut impl FnMut(&mut Expression, &mut T) -> bool) -> bool {
            use Expression::*;
            match expr {
                LetLiteral { body, .. } => {
                    visit_children(body.as_mut(), a, f) && f(body.as_mut(), a)
                },
                LetLambda { val, body, .. } => {
                    visit_children(val.body.as_mut(), a, f) && f(val.body.as_mut(), a) && visit_children(body.as_mut(), a, f) && f(body.as_mut(), a)
                },
                LetContinuation { k, body, ..} => {
                    (if let Continuation::Implicit { body: k_body, .. } = k {
                        visit_children(k_body.as_mut(), a, f) && f(k_body.as_mut(), a)
                    } else {
                        true
                    }) && visit_children(body.as_mut(), a, f) && f(body.as_mut(), a)
                },
                Branch { consequent, alternate, .. } => {
                    visit_children(consequent.as_mut(), a, f) && f(consequent.as_mut(), a) &&
                    visit_children(alternate.as_mut(), a, f) && f(alternate.as_mut(), a)
                },
                Continue { k, .. } | Assign { k, .. } => {
                    if let Continuation::Implicit { body: k_body, .. } = k {
                        visit_children(k_body.as_mut(), a, f) && f(k_body.as_mut(), a)
                    } else {
                        true
                    }
                },
                Apply { .. } | Halt { .. } | Abort { .. } | HaltEmpty => true,
            }
       }

       let res = visit_children(self, &mut accumulator, &mut f) && f(self, &mut accumulator);
       (accumulator, res)
    }
    fn visit<T>(&self, mut accumulator: T, mut f: impl Fn(&Self, &mut T) -> bool) -> (T, bool) {
        fn visit_children<T>(expr: &Expression, a: &mut T, f: &mut impl FnMut(&Expression, &mut T) -> bool) -> bool {
            use Expression::*;
            match expr {
                LetLiteral { body, .. } => {
                    visit_children(body.as_ref(), a, f) && f(body.as_ref(), a)
                },
                LetLambda { val, body, .. } => {
                    visit_children(val.body.as_ref(), a, f) && f(val.body.as_ref(), a) && visit_children(body.as_ref(), a, f) && f(body.as_ref(), a)
                },
                LetContinuation { k, body, ..} => {
                    (if let Continuation::Implicit { body: k_body, .. } = k {
                        visit_children(k_body.as_ref(), a, f) && f(k_body.as_ref(), a)
                    } else {
                        true
                    }) && visit_children(body.as_ref(), a, f) && f(body.as_ref(), a)
                },
                Branch { consequent, alternate, .. } => {
                    visit_children(consequent.as_ref(), a, f) && f(consequent.as_ref(), a) &&
                    visit_children(alternate.as_ref(), a, f) && f(alternate.as_ref(), a)
                },
                Continue { k, .. } | Assign { k, .. } => {
                    if let Continuation::Implicit { body: k_body, .. } = k {
                        visit_children(k_body.as_ref(), a, f) && f(k_body.as_ref(), a)
                    } else {
                        true
                    }
                },
                Apply { .. } | Halt { .. } | Abort { .. } | HaltEmpty => true,
            }
       }

       let res = visit_children(self, &mut accumulator, &mut f) && f(self, &mut accumulator);
       (accumulator, res)
    }
    fn visit_mut(&mut self, mut f: impl FnMut(&mut Self) -> bool) -> bool {
        self.visit_multi((), |e, _| f(e)).1
    }

    fn rename(self, from: &(Vec<ValueID>, Option<ValueID>), to: &(Vec<Atom>, Option<Atom>)) -> Self {
        use Expression::*;
        let mapping = std::iter::zip(from.0.iter().copied(), to.0.iter().copied()).collect::<std::collections::HashMap<_, _>>();
        let map_atom = |a| if let Atom::Variable(i) = a { mapping.get(&i).copied().unwrap_or(a) } else { a };
        let from_atom = (from.0.iter().copied().map(Atom::Variable).collect(), from.1.map(Atom::Variable));
        match self {
            LetLiteral { var, val, body } => LetLiteral {
                var,
                val,
                body: Box::new(body.rename(from, to)),
            },
            LetLambda { var, val, body } => LetLambda {
                var,
                val: Lambda {
                    formals: val.formals,
                    k: val.k,
                    kf: val.kf,
                    body: Box::new(val.body.rename(from, to)),
                },
                body: Box::new(body.rename(from, to)),
            },
            LetContinuation { var, k, body } => LetContinuation {
                var,
                k: k.rename(from, to),
                body: Box::new(body.rename(from, to)),
            },
            Apply { operator, operands, k, kf } => Apply {
                operator: map_atom(operator),
                operands: (operands.0.into_iter().map(map_atom).collect(), operands.1.map(map_atom)),
                k,
                kf,
            },
            Branch { test, consequent, alternate } => Branch {
                test: map_atom(test),
                consequent: Box::new(consequent.rename(from, to)),
                alternate: Box::new(alternate.rename(from, to)),
            },
            Continue { k, values } => Continue {
                k: k.rename(from, to),
                values: (values.0.into_iter().map(map_atom).collect(), values.1.map(map_atom)),
            },
            Assign { vars, values, k } => Assign {
                vars,
                values: if values == from_atom { to.clone() } else { (values.0.into_iter().map(map_atom).collect(), values.1.map(map_atom)) },
                k: k.rename(from, to),
            },
            Halt { values } => Halt { values: values.into_iter().map(map_atom).collect() },
            Abort { values } => Abort { values: values.into_iter().map(map_atom).collect() },
            HaltEmpty => HaltEmpty,
        }
    }

    fn rename_continues(self, from: &[ContinuationID], to: &[ContinuationID]) -> Self {
        use Expression::*;
        let mapping = std::iter::zip(from.iter().copied(), to.iter().copied()).collect::<std::collections::HashMap<_, _>>();
        let map_atom = |a| if let Atom::Continuation(i) = a { Atom::Continuation(mapping.get(&i).copied().unwrap_or(i)) } else { a };
        match self {
            LetLiteral { var, val, body } => LetLiteral {
                var,
                val,
                body: Box::new(body.rename_continues(from, to)),
            },
            LetLambda { var, val, body } => LetLambda {
                var,
                val: Lambda {
                    formals: val.formals,
                    k: mapping.get(&val.k).copied().unwrap_or(val.k),
                    kf: mapping.get(&val.kf).copied().unwrap_or(val.kf),
                    body: Box::new(val.body.rename_continues(from, to)),
                },
                body: Box::new(body.rename_continues(from, to)),
            },
            LetContinuation { var, k, body } => LetContinuation {
                var,
                k: k.rename_continues(from, to),
                body: Box::new(body.rename_continues(from, to)),
            },
            Apply { operator, operands, k, kf } => Apply {
                operator: map_atom(operator),
                operands: (operands.0.into_iter().map(map_atom).collect(), operands.1.map(map_atom)),
                k: mapping.get(&k).copied().unwrap_or(k),
                kf: mapping.get(&kf).copied().unwrap_or(kf),
            },
            Branch { test, consequent, alternate } => Branch {
                test, // if test is Atom::Continuation, this will already have been optimized out
                consequent: Box::new(consequent.rename_continues(from, to)),
                alternate: Box::new(alternate.rename_continues(from, to)),
            },
            Continue { k, values } => Continue {
                k: k.rename_continues(from, to),
                values: (values.0.into_iter().map(map_atom).collect(), values.1.map(map_atom)),
            },
            Assign { vars, values, k } => Assign {
                vars,
                values: (values.0.into_iter().map(map_atom).collect(), values.1.map(map_atom)),
                k: k.rename_continues(from, to),
            },
            Halt { values } => Halt { values: values.into_iter().map(map_atom).collect() },
            Abort { values } => Abort { values: values.into_iter().map(map_atom).collect() },
            HaltEmpty => HaltEmpty,
        }
    }

    fn uses_variable(&self, v: ValueID) -> bool {
        use Expression::*;
        match self {
            LetLiteral { var, body, .. } => *var == v || body.uses_variable(v),
            LetLambda { var, val, body } => *var == v || val.uses_variable(v) || body.uses_variable(v),
            LetContinuation { k, body, .. } => k.uses_variable(v) || body.uses_variable(v),
            Apply { operator, operands, .. } => *operator == Atom::Variable(v) || operands.0.iter().chain(operands.1.iter()).any(|i| *i == Atom::Variable(v)),
            Branch { test, consequent, alternate } => *test == Atom::Variable(v) || consequent.uses_variable(v) || alternate.uses_variable(v),
            Continue { k, values } => k.uses_variable(v) || values.0.iter().chain(values.1.iter()).any(|i| *i == Atom::Variable(v)),
            Assign { vars, values, k } => vars.0.iter().chain(vars.1.iter()).any(|i| *i == v) || values.0.iter().chain(values.1.iter()).any(|i| *i == Atom::Variable(v)) || k.uses_variable(v),
            Halt { values } => values.contains(&Atom::Variable(v)),
            Abort { values } => values.contains(&Atom::Variable(v)),
            HaltEmpty => false,
        }
    }

    fn uses_continuation(&self, v: ContinuationID) -> bool {
        use Expression::*;
        match self {
            LetLiteral { body, .. } => body.uses_continuation(v),
            LetLambda { val, body, .. } => val.uses_continuation(v) || body.uses_continuation(v),
            LetContinuation { var, k, body } => *var == v || k.uses_continuation(v) || body.uses_continuation(v),
            Apply { operator, operands, k, kf } => *operator == Atom::Continuation(v) || operands.0.iter().chain(operands.1.iter()).any(|i| *i == Atom::Continuation(v)) || *k == v || *kf == v,
            Branch { test, consequent, alternate } => *test == Atom::Continuation(v) || consequent.uses_continuation(v) || alternate.uses_continuation(v),
            Continue { k, values } => k.uses_continuation(v) || values.0.iter().chain(values.1.iter()).any(|i| *i == Atom::Continuation(v)),
            Assign { values, k, .. } => values.0.iter().chain(values.1.iter()).any(|i| *i == Atom::Continuation(v)) || k.uses_continuation(v),
            Halt { values } => values.contains(&Atom::Continuation(v)),
            Abort { values } => values.contains(&Atom::Continuation(v)),
            HaltEmpty => false,
        }
    }
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
                    k: Continuation::Implicit { formals: (vec![x], None), body: Box::new(then(Atom::Variable(x))) },
                    body: Box::new(self.cps_transform(e, k, kf)),
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
                        k: Continuation::Implicit { formals: (vec![id], None), body: Box::new(acc), },
                        body: Box::new(self.cps_transform(e, kx, kf)),
                    }
                }
            })
    }

    /// Transforms `e` into continuation-passing style and invokes `k` on it.
    /// Exceptions are handled by calling `kf`.
    fn cps_transform(&self, e: syntax::Expression, k: ContinuationID, kf: ContinuationID) -> Expression {
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
                    self.atomize_many(operands, kf, move |operands| Expression::Apply {
                        operator,
                        operands: (operands, None),
                        k,
                        kf,
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
                self.atomize(*test, kf, move |test| Expression::Branch {
                    test,
                    consequent: Box::new(self.cps_transform(*consequent, k, kf)),
                    alternate: Box::new(match alternate {
                        None => Expression::Continue {
                            k: Continuation::Variable(k),
                            values: (vec![Atom::Literal(LiteralC::Boolean(false))], None),
                        },
                        Some(alternate) => self.cps_transform(*alternate, k, kf),
                    }),
                })
            }
            syntax::Expression::Assignment { id, value } => {
                self.atomize(*value, kf, move |val| Expression::Assign {
                    vars: (vec![ValueID::from(id.as_ref())], None),
                    values: (vec![val], None),
                    k: Continuation::Variable(k),
                })
            },
            syntax::Expression::Definition { formals, body } => {
                let x = ValueID(self.new_variable());
                let kx = ContinuationID(self.new_variable());
                Expression::LetContinuation {
                    var: kx,
                    k: Continuation::Implicit {
                        formals: (vec![], Some(x)),
                        body: Box::new(Expression::Assign {
                            vars: (formals.0.iter().map(|e| ValueID::from(e)).collect(), formals.1.map(|e| ValueID::from(e.as_ref()))),
                            values: (vec![], Some(Atom::Variable(x))),
                            k: Continuation::Variable(k),
                        }),
                    },
                    body: Box::new(self.cps_transform(*body, kx, kf)),
                }
            }
            syntax::Expression::Block { body } => self.transform_block(body, k, kf),
        }
    }
    fn transform_block(&self, e: Vec<syntax::Expression>, k: ContinuationID, kf: ContinuationID) -> Expression {
        let mut ee = e.into_iter().rev();
        if let Some(tail) = ee.next() {
            ee.fold(self.cps_transform(tail, k, kf), |acc, e| {
                let kx = ContinuationID(self.new_variable());
                Expression::LetContinuation {
                    var: kx,
                    k: Continuation::Implicit {
                        formals: (vec![], Some(ValueID(self.new_variable()))),
                        body: Box::new(acc),
                    },
                    body: Box::new(self.cps_transform(e, kx, kf)),
                }
            })
        } else {
            Expression::Continue { k: Continuation::Variable(k), values: (vec![], None) }
        }
    }

    fn optimize_cps(&self, mut e: Expression) -> Expression {
        loop {
            if e.visit_mut(|e| {
                use Expression::*;
                match e {
                    LetLiteral { var, body, .. } => {
                        if body.uses_variable(*var) {
                            true
                        } else {
                            *e = std::mem::take(body);
                            false
                        }
                    },
                    LetLambda { var, val, body } => {
                        let mut ret = true;
                        let (applications, _) = body.visit(0, |e, applications| match e {
                            Apply { operator: Atom::Variable(operator), .. } if operator == var => match *applications {
                                0 => { *applications = 1; true },
                                i => { *applications = i + 1; false },
                            },
                            _ => true,
                        });
                        if applications == 1 {
                            ret &= body.visit_mut(|e| match e {
                                Apply { operator: Atom::Variable(operator), operands, k, kf } if operator == var => {
                                    *e = std::mem::take(val.body.as_mut())
                                        .rename_continues(&[val.k, val.kf], &[*k, *kf])
                                        .rename(&val.formals, &operands);
                                    false
                                },
                                _ => true,
                            });
                        }
                        if body.uses_variable(*var) {
                            ret
                        } else {
                            *e = std::mem::take(body);
                            false
                        }
                    }
                    LetContinuation { var, k, body } => {
                        let mut modified = false;
                        body.visit_mut(|e| {
                            if let Continue { k: Continuation::Variable(k_id), values } = e {
                                if k_id == var {
                                    modified = true;
                                    *e = match k {
                                        Continuation::Variable(i) => Continue {
                                            k: Continuation::Variable(*i),
                                            values: std::mem::take(values)
                                        },
                                        Continuation::Implicit { formals, body } => body.clone().rename(&formals, &values),
                                        Continuation::OptimizedOut => unreachable!(),
                                    };
                                }
                            }
                            true
                        });

                        if body.uses_continuation(*var) {
                            !modified
                        } else {
                            *e = std::mem::take(body);
                            false
                        }
                    }
                    Apply { operator, operands, k, kf } => {
                        match operator {
                            Atom::Continuation(k_id) => {
                                *e = Continue {
                                    k: Continuation::Variable(*k_id),
                                    values: std::mem::take(operands),
                                };
                                false
                            },
                            Atom::CorePrimitive("apply") => {
                                if operands.0.len() >= 1 {
                                    *e = Apply {
                                        operator: operands.0.first().copied().unwrap(),
                                        operands: (operands.0.iter().skip(1).copied().collect(), operands.1),
                                        k: *k,
                                        kf: *kf,
                                    };
                                } else {
                                    let x0 = ValueID(self.new_variable());
                                    let x1 = ValueID(self.new_variable());
                                    *e = Continue {
                                        k: Continuation::Implicit {
                                            formals: (vec![x0], Some(x1)),
                                            body: Box::new(Apply {
                                                operator: Atom::Variable(x0),
                                                operands: (vec![], Some(Atom::Variable(x1))),
                                                k: *k,
                                                kf: *kf,
                                            }),
                                        },
                                        values: std::mem::take(operands),
                                    };
                                }
                                false
                            },
                            Atom::CorePrimitive("call/cc" | "call-with-current-continuation") => {
                                if operands.0.len() >= 1 {
                                    *e = Apply {
                                        operator: operands.0.first().copied().unwrap(),
                                        operands: (vec![Atom::Continuation(*k)], None),
                                        k: *k,
                                        kf: *kf,
                                    };
                                } else {
                                    let x0 = ValueID(self.new_variable());
                                    *e = Continue {
                                        k: Continuation::Implicit {
                                            formals: (vec![x0], None),
                                            body: Box::new(Apply {
                                                operator: Atom::Variable(x0),
                                                operands: (vec![Atom::Continuation(*k)], None),
                                                k: *k,
                                                kf: *kf,
                                            }),
                                        },
                                        values: std::mem::take(operands),
                                    };
                                }
                                false
                            },
                            Atom::CorePrimitive("call-with-values") => {
                                let x0 = ValueID(self.new_variable());
                                let k0 = ContinuationID(self.new_variable());
                                let cwv = |operands: &[Atom]| LetContinuation {
                                    var: k0,
                                    k: Continuation::Implicit {
                                        formals: (vec![], Some(x0)),
                                        body: Box::new(Apply {
                                            operator: operands[1],
                                            operands: (vec![], Some(Atom::Variable(x0))),
                                            k: *k,
                                            kf: *kf,
                                        }),
                                    },
                                    body: Box::new(Apply {
                                        operator: operands[0],
                                        operands: (vec![], None),
                                        k: k0,
                                        kf: *kf,
                                    }),
                                };
                                if operands.0.len() >= 2 {
                                    *e = cwv(&operands.0);
                                } else {
                                    let formals = (0..2).map(|_| ValueID(self.new_variable())).collect::<Vec<_>>();
                                    *e = Continue {
                                        k: Continuation::Implicit {
                                            formals: (formals.clone(), None),
                                            body: Box::new(cwv(&formals.into_iter().map(Atom::Variable).collect::<Vec<_>>())),
                                        },
                                        values: std::mem::take(operands),
                                    };
                                }
                                false
                            },
                            Atom::CorePrimitive("dynamic-wind") => {
                                let k0 = ContinuationID(self.new_variable());
                                let k1 = ContinuationID(self.new_variable());
                                let dw = |operands: &[Atom]| LetContinuation {
                                    var: k0,
                                    k: Continuation::Implicit {
                                        formals: (vec![], None),
                                        body: Box::new(Apply {
                                            operator: operands[0],
                                            operands: (vec![], None),
                                            k: *k,
                                            kf: *kf
                                        })
                                    },
                                    body: Box::new(LetContinuation {
                                        var: k1,
                                        k: Continuation::Implicit {
                                            formals: (vec![], None),
                                            body: Box::new(Apply {
                                                operator: Atom::CorePrimitive("__push_dynamic_frame"),
                                                operands: (vec![operands[0], operands[2]], None),
                                                k: k0,
                                                kf: *kf,
                                            }),
                                        },
                                        body: Box::new(Apply {
                                            operator: operands[0],
                                            operands: (vec![], None),
                                            k: k1,
                                            kf: *kf
                                        }),
                                    }),
                                };

                                if operands.0.len() >= 3 {
                                    *e = dw(&operands.0);
                                } else {
                                    let formals = (0..3).map(|_| ValueID(self.new_variable())).collect::<Vec<_>>();
                                    *e = Continue {
                                        k: Continuation::Implicit {
                                            formals: (formals.clone(), None),
                                            body: Box::new(dw(&formals.into_iter().map(Atom::Variable).collect::<Vec<_>>())),
                                        },
                                        values: std::mem::take(operands),
                                    };
                                }
                                false
                            },
                            Atom::CorePrimitive("values") => {
                                *e = Continue {
                                    k: Continuation::Variable(*k),
                                    values: std::mem::take(operands),
                                };
                                false
                            },
                            Atom::CorePrimitive("raise" | "raise-continuable" | "with-exception-handler") => todo!(),
                            _ if operands.0.iter().all(|a| matches!(a, Atom::Literal(_))) && operands.1.is_none() => match operator {
                                _ => true,
                            },
                            _ => true,
                        }
                    },
                    Branch { test, consequent, alternate } => match test {
                        Atom::Literal(LiteralC::Boolean(false)) => { *e = std::mem::take(alternate); false },
                        Atom::Literal(_) | Atom::Continuation(_) | Atom::CorePrimitive(_) => { *e = std::mem::take(consequent); false },
                        _ => true,
                    }
                    _ => true,
                }
            }) { break; }
        }

        e
    }
}

pub fn transform(e: syntax::Expression, k: ContinuationID, kf: ContinuationID, env: &crate::Environment) -> Expression {
    let e = env.cps_transform(e, k, kf);
    eprintln!("step4: {:?}", e);
    env.optimize_cps(e)
}
