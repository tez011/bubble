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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Continuation {
    #[default] Halt,
    Abort,
    To(ContinuationID),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Atom {
    Literal(LiteralC),
    Variable(ValueID),
    Continuation(Continuation),
    CorePrimitive(&'static str),
}

#[derive(Debug, Clone, Default)]
pub struct Lambda {
    /// The defined parameters of this function.
    formals: (Vec<ValueID>, Option<ValueID>),
    /// The continuation to invoke with the return value.
    k: Continuation,
    body: Box<Expression>,
}

#[derive(Debug, Clone, Default)]
pub struct ClosedLambda {
    /// Ordered parameters representing the closed environment of the lambda.
    closed_env: Vec<ValueID>,
    /// The defined parameters of this function.
    formals: (Vec<ValueID>, Option<ValueID>),
    /// The continuation to invoke with the return value.
    k: Continuation,
    body: Box<Expression>,
}
impl From<Lambda> for ClosedLambda {
    fn from(value: Lambda) -> Self {
        let closed_env = value.body.variables()
            .difference(&value.formals.0.iter().chain(value.formals.1.iter()).copied().collect())
            .copied().collect::<Vec<_>>();
        ClosedLambda {
            closed_env,
            formals: value.formals,
            k: value.k,
            body: value.body,
        }
    }
}


#[derive(Debug, Clone, Default)]
pub enum Expression {
    /// Identical to `body`, but with the literal `val` bound to the variable index `var`.
    /// `pure` if the only use of `val` is within `body`.
    LetLiteral { id: ValueID, val: LiteralD, body: Box<Expression>, pure: bool },
    /// Identical to `body`, but with the lambda `val` bound to the variable index `var`.
    /// `pure` if the only use of `val` is within `body`.
    LetLambda { id: ValueID, val: Lambda, body: Box<Expression>, pure: bool },
    /// Equivalent to LetLambda, but the lambda has been hoisted away.
    LetClosure { id: ValueID, index: usize, closed_env: Vec<ValueID>, body: Box<Expression> },
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
    #[default] TakenOut,
}

impl Expression {
    /// With exclusive access to an `Expression`, visit each of its children, depth-first, while
    /// repeatedly applying this operation to the accumulator.
    /// `f` returns false if the visit operation should abort.
    /// Returns the final value of the accumulator, and whether the visit operation completed after
    /// visiting all children.
    fn visit_multi<T>(&mut self, mut accumulator: T, mut f: impl FnMut(&mut T, &mut Self) -> bool) -> (T, bool) {
        fn visit_children<T>(expr: &mut Expression, a: &mut T, f: &mut impl FnMut(&mut T, &mut Expression) -> bool) -> bool {
            use Expression::*;
            match expr {
                LetLiteral { body, .. } | LetClosure { body, .. } => {
                    visit_children(body.as_mut(), a, f) && f(a, body.as_mut())
                },
                LetLambda { val, body, .. } => {
                    visit_children(val.body.as_mut(), a, f) && f(a, val.body.as_mut()) && visit_children(body.as_mut(), a, f) && f(a, body.as_mut())
                },
                LetContinuation { k, body, .. } => {
                    visit_children(k.as_mut(), a, f) && f(a, k.as_mut()) &&
                    visit_children(body.as_mut(), a, f) && f(a, body.as_mut())
                },
                Branch { consequent, alternate, .. } => {
                    visit_children(consequent.as_mut(), a, f) && f(a, consequent.as_mut()) &&
                    visit_children(alternate.as_mut(), a, f) && f(a, alternate.as_mut())
                },
                Continue { .. } | Assign { .. } | Apply { .. } | TakenOut => true,
            }
       }

       let res = visit_children(self, &mut accumulator, &mut f) && f(&mut accumulator, self);
       (accumulator, res)
    }

    /// Visit all the children of an `Expression`, depth-first, while repeatedly applying this
    /// operation to the accumulator.
    /// `f` returns false if the visit operation should abort.
    /// Returns the final value of the accumulator, and whether the visit operation completed after
    /// visiting all children.
    fn visit<T>(&self, mut accumulator: T, mut f: impl FnMut(&mut T, &Self) -> bool) -> (T, bool) {
        fn visit_children<T>(expr: &Expression, a: &mut T, f: &mut impl FnMut(&mut T, &Expression) -> bool) -> bool {
            use Expression::*;
            match expr {
                LetLiteral { body, .. } | LetClosure { body, .. } => {
                    visit_children(body.as_ref(), a, f) && f(a, body.as_ref())
                },
                LetLambda { val, body, .. } => {
                    visit_children(val.body.as_ref(), a, f) && f(a, val.body.as_ref()) && visit_children(body.as_ref(), a, f) && f(a, body.as_ref())
                },
                LetContinuation { k, body, .. } => {
                    visit_children(k.as_ref(), a, f) && f(a, k.as_ref()) &&
                    visit_children(body.as_ref(), a, f) && f(a, body.as_ref())
                },
                Branch { consequent, alternate, .. } => {
                    visit_children(consequent.as_ref(), a, f) && f(a, consequent.as_ref()) &&
                    visit_children(alternate.as_ref(), a, f) && f(a, alternate.as_ref())
                },
                Continue { .. } | Assign { .. } | Apply { .. } | TakenOut => true,
            }
       }

       let res = visit_children(self, &mut accumulator, &mut f) && f(&mut accumulator, self);
       (accumulator, res)
    }

    /// With exclusive access to an `Expression`, visit each of its children, depth-first.
    /// `f` returns false if the visit operation should abort.
    /// Returns whether the visit operation completed after visiting all children.
    fn visit_mut(&mut self, mut f: impl FnMut(&mut Self) -> bool) -> bool {
        self.visit_multi((), |_, e| f(e)).1
    }

    fn complexity(&self) -> usize {
        self.visit(0, |count, _| {
            *count += 1;
            true
        }).0
    }

    fn uses_variable(&self, id: ValueID) -> usize {
        use Expression::*;
        self.visit(0, |count, e| {
            *count += match e {
                Apply { operator, operands, .. } => std::iter::once(operator).chain(operands.0.iter()).chain(operands.1.iter()).filter(|&&a| a == Atom::Variable(id)).count(),
                Branch { test, .. } => if *test == Atom::Variable(id) { 1 } else { 0 },
                Continue { values, .. } | Assign { values, .. } => values.0.iter().chain(values.1.iter()).filter(|&&a| a == Atom::Variable(id)).count(),
                _ => 0,
            };
            true
        }).0
    }

    fn uses_continuation(&self, id: ContinuationID) -> usize {
        use Expression::*;
        self.visit(0, |count, e| {
            if match e {
                LetLambda { val: Lambda { k, .. }, .. } => *k == Continuation::To(id),
                Apply { k, .. } | Continue { k, .. } | Assign { k, .. } => *k == Continuation::To(id),
                _ => false,
            } {
                *count += 1;
            }
            true
        }).0
    }

    fn variables(&self) -> std::collections::BTreeSet<ValueID> {
        let atom2var = |atom: &Atom| if let Atom::Variable(i) = atom { Some(*i) } else { None };
        let mut uses = std::collections::BTreeSet::new();
        let mut intros = std::collections::BTreeSet::new();
        let mut queue = std::iter::once(self).collect::<std::collections::VecDeque<_>>();
        while let Some(e) = queue.pop_front() {
            use Expression::*;
            match e {
                LetLiteral { id, body, .. } => {
                    intros.insert(*id);
                    queue.push_back(body.as_ref());
                },
                LetLambda { id, val, body, .. } => {
                    intros.insert(*id);
                    intros.extend(val.formals.0.iter().chain(val.formals.1.iter()));
                    queue.push_back(body.as_ref());
                },
                LetClosure { id, body, .. } => {
                    intros.insert(*id);
                    queue.push_back(body.as_ref());
                },
                LetContinuation { kf, k, body, .. } => {
                    intros.extend(kf.0.iter().chain(kf.1.iter()));
                    queue.push_back(k.as_ref());
                    queue.push_back(body.as_ref());
                },
                Apply { operator, operands, .. } => {
                    uses.extend(std::iter::once(operator)
                        .chain(operands.0.iter())
                        .chain(operands.1.iter())
                        .filter_map(atom2var));
                },
                Branch { test, consequent, alternate } => {
                    if let Atom::Variable(test) = test {
                        uses.insert(*test);
                    }
                    queue.push_back(consequent.as_ref());
                    queue.push_back(alternate.as_ref());
                },
                Continue { values, .. } => {
                    uses.extend(values.0.iter().chain(values.1.iter()).filter_map(atom2var));
                },
                Assign { vars, values, .. } => {
                    intros.extend(vars.0.iter().chain(vars.1.iter())); // The value is being introduced to this variable here ...
                    uses.extend(values.0.iter().chain(values.1.iter()).filter_map(atom2var));
                },
                TakenOut => (),
            }
        }

        uses.difference(&intros).copied().collect()
    }

    fn rename(&mut self, from: &(Vec<ValueID>, Option<ValueID>), to: &(Vec<Atom>, Option<Atom>)) -> bool {
        eprintln!("eta: {:?} -> {:?}", from, to);

        use Expression::*;
        let mapping = std::iter::zip(from.0.iter().copied(), to.0.iter().copied()).collect::<std::collections::HashMap<_, _>>();
        let map_atom = |a: &mut Atom| { if let Atom::Variable(i) = a { if let Some(&mapped) = mapping.get(&i) { *a = mapped; return true; } }; false };
        let from_atom = (from.0.iter().copied().map(Atom::Variable).collect::<Vec<_>>(), from.1.map(Atom::Variable));
        self.visit_multi(false, |acc, e| {
            match e {
                Apply { operator, operands, .. } => {
                    *acc |= operands.0.iter_mut()
                        .chain(operands.1.iter_mut())
                        .chain(std::iter::once(operator))
                        .any(map_atom);
                },
                Branch { test, .. } => *acc |= map_atom(test),
                Continue { values, .. } | Assign { values, .. } if *values == from_atom => {
                    *values = to.clone();
                    *acc = true;
                },
                Continue { values, .. } | Assign { values, .. } => {
                    *acc |= values.0.iter_mut().chain(values.1.iter_mut()).any(map_atom);
                },
                _ => (),
            }
            true
        }).0
    }
    fn rename_k(&mut self, from: Continuation, to: Continuation) -> bool {
        use Expression::*;
        let map_k = |k: &mut Continuation| if *k == from { *k = to; true } else { false };
        let map_atom = |a: &mut Atom| { if let Atom::Continuation(i) = a { if *i == from { *a = Atom::Continuation(to); return true; } }; false };
        self.visit_multi(false, |acc, e| {
            match e {
                LetLambda { val: Lambda { k, .. }, .. } => *acc |= map_k(k),
                Apply { operator, operands, k } => {
                    *acc |= operands.0.iter_mut()
                        .chain(operands.1.iter_mut())
                        .chain(std::iter::once(operator))
                        .any(map_atom);
                    *acc |= map_k(k);
                },
                Branch { test, .. } => *acc |= map_atom(test),
                Continue { k, values, .. } | Assign { values, k, .. } => {
                    *acc |= values.0.iter_mut().chain(values.1.iter_mut()).any(map_atom);
                    *acc |= map_k(k);
                },
                _ => (),
            }
            true
        }).0
    }

    /// Atomizes an expression `e` so it can be used in other expressions.
    /// If the expression is not atomic, its value is bound to a new variable.
    /// Returns an expression that is the result of using the atom (with `then`), whether directly,
    /// or after binding it to a variable.
    fn atomize(e: syntax::Expression, env: &crate::Environment, then: impl FnOnce(Atom) -> Expression) -> Self {
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
    fn atomize_many(ee: Vec<syntax::Expression>, env: &crate::Environment, then: impl FnOnce(Vec<Atom>) -> Expression) -> Self {
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
    fn new(e: syntax::Expression, k: Continuation, env: &crate::Environment) -> Self {
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
                    pure: true
                }
            },
            stx::ProcedureCall { operator, operands } => {
                Self::atomize(*operator, env, move |operator|
                    Self::atomize_many(operands, env, move |operands| Apply {
                        operator,
                        operands: (operands, None),
                        k,
                    }))
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
                    pure: true,
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
                let x = ValueID(env.new_variable());
                let kx = ContinuationID(env.new_variable());
                LetContinuation {
                    id: kx,
                    kf: (vec![], Some(x)),
                    k: Box::new(Assign {
                        vars: (ids.0.iter().map(ValueID::from).collect(), ids.1.map(|e| ValueID::from(e.as_ref()))),
                        values: (vec![], Some(Atom::Variable(x))),
                        k,
                    }),
                    body: Box::new(Self::new(*value, Continuation::To(kx), env)),
                }
            },
            stx::Block { body } => Self::new_block(body, k, env),
        }
    }
    fn new_block(e: Vec<syntax::Expression>, k: Continuation, env: &crate::Environment) -> Self {
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

    fn optimize(&mut self, env: &crate::Environment) {
        while !self.visit_mut(|e| {
            eprintln!("optimize: {:?}", e);
            e.optimize_once(env)
        }) {
            eprintln!("*** optimize starting over");
        }
    }
    fn optimize_once(&mut self, env: &crate::Environment) -> bool {
        use Expression::*;
        match self {
            LetLiteral { id, body, pure, .. } => {
                let uses = body.uses_variable(*id);
                if *pure && uses == 0 {
                    *self = std::mem::take(body);
                    return false;
                }
                if *pure && uses == 1 {
                    return body.visit_mut(|e| {
                        if let Assign { vars, values, .. } = e {
                            if let Some(index) = values.0.iter().enumerate().find_map(|(i, &v)| if v == Atom::Variable(*id) { Some(i) } else { None }) {
                                values.0.swap_remove(index);
                                *id = vars.0.swap_remove(index);
                                *pure = false;
                                return false;
                            }
                        }
                        true
                    });
                }
                true
            },
            LetLambda { id, val, body, pure, .. } => {
                let uses = body.uses_variable(*id);
                if *pure && uses == 0 {
                    *self = std::mem::take(body);
                    false
                } else if *pure && uses == 1 {
                    body.visit_mut(|e| {
                        if let Assign { vars, values, .. } = e {
                            if let Some(index) = values.0.iter().enumerate().find_map(|(i, &v)| if v == Atom::Variable(*id) { Some(i) } else { None }) {
                                values.0.swap_remove(index);
                                *id = vars.0.swap_remove(index);
                                *pure = false;
                                return false;
                            }
                        } else if let Apply { operator: Atom::Variable(operator), operands, k } = e {
                            if operator == id {
                                if val.body.rename(&val.formals, &operands) | val.body.rename_k(val.k, *k) {
                                    *e = std::mem::take(&mut val.body);
                                    return false;
                                }
                            }
                        }
                        true
                    })
                } else if body.complexity() < 5 {
                    let mut modified = false;
                    body.visit_mut(|e| {
                        if let Apply { operator: Atom::Variable(operator), operands, k } = e {
                            if operator == id {
                                modified = true;
                                let mut n = val.body.clone();
                                n.rename(&val.formals, &operands);
                                n.rename_k(val.k, *k);
                                *e = *n;
                            }
                        }
                        true
                    });
                    !modified
                } else {
                    true
                }
            },
            LetContinuation { id, kf, k, body, .. } => {
                if body.uses_continuation(*id) == 0 {
                    *self = std::mem::take(body);
                    false
                } else if body.uses_continuation(*id) == 1 {
                    let r = body.visit_mut(|e| {
                        if let Continue { k: Continuation::To(k_id), values } = e {
                            if k_id == id {
                                if k.rename(kf, values) {
                                    *e = std::mem::take(k);
                                    return false;
                                }
                            }
                        }
                        true
                    });
                    if r == false {
                        *self = std::mem::take(body);
                    }
                    r
                } else {
                    true
                }
            },
            Apply { operator, operands, k } => match *operator {
                Atom::CorePrimitive("apply") => {
                    let proc = ValueID(env.new_variable());
                    let args = ValueID(env.new_variable());
                    let k0 = ContinuationID(env.new_variable());
                    *self = LetContinuation {
                        id: k0,
                        kf: (vec![proc], Some(args)),
                        k: Box::new(Apply {
                            operator: Atom::Variable(proc),
                            operands: (vec![], Some(Atom::Variable(args))),
                            k: *k,
                        }),
                        body: Box::new(Continue { k: Continuation::To(k0), values: std::mem::take(operands) }),
                    };
                    false
                },
                Atom::CorePrimitive("call/cc" | "call-with-current-continuation") => {
                    let dynamic_extent = ValueID(env.new_variable());
                    let proc = ValueID(env.new_variable());
                    let args = ValueID(env.new_variable());
                    let reified_cont = ValueID(env.new_variable());
                    let k0 = ContinuationID(env.new_variable());
                    let k1 = ContinuationID(env.new_variable());
                    let k2 = ContinuationID(env.new_variable());
                    *self = LetContinuation {
                        id: k0,
                        kf: (vec![dynamic_extent], None),
                        k: Box::new(LetContinuation {
                            id: k1,
                            kf: (vec![proc], None),
                            k: Box::new(LetLambda {
                                id: reified_cont,
                                val: Lambda {
                                    formals: (vec![], Some(args)),
                                    k: Continuation::Halt,
                                    body: Box::new(LetContinuation {
                                        id: k2,
                                        kf: (vec![], None),
                                        k: Box::new(Continue { k: *k, values: (vec![], Some(Atom::Variable(args))) }),
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
                                    k: *k,
                                }),
                                pure: true,
                            }),
                            body: Box::new(Continue { k: Continuation::To(k1), values: std::mem::take(operands) }),
                        }),
                        body: Box::new(Apply {
                            operator: Atom::CorePrimitive("__store_dynamic_extent"),
                            operands: (vec![], None),
                            k: Continuation::To(k0),
                        }),
                    };
                    false
                },
                Atom::CorePrimitive("call-with-values") => {
                    let producer = ValueID(env.new_variable());
                    let consumer = ValueID(env.new_variable());
                    let the_values = ValueID(env.new_variable());
                    let k0 = ContinuationID(env.new_variable());
                    let k1 = ContinuationID(env.new_variable());
                    *self = LetContinuation {
                        id: k0,
                        kf: (vec![producer, consumer], None),
                        k: Box::new(LetContinuation {
                            id: k1,
                            kf: (vec![], Some(the_values)),
                            k: Box::new(Apply {
                                operator: Atom::Variable(consumer),
                                operands: (vec![], Some(Atom::Variable(the_values))),
                                k: *k,
                            }),
                            body: Box::new(Apply {
                                operator: Atom::Variable(producer),
                                operands: (vec![], None),
                                k: Continuation::To(k1),
                            }),
                        }),
                        body: Box::new(Continue { k: Continuation::To(k0), values: std::mem::take(operands) }),
                    };
                    false
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
                    *self = LetContinuation {
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
                                        k: *k,
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
                        body: Box::new(Continue { k: Continuation::To(k0), values: std::mem::take(operands) }),
                    };
                    false
                },
                Atom::CorePrimitive("values") => {
                    *self = Continue { k: *k, values: std::mem::take(operands) };
                    false
                },
                Atom::CorePrimitive("raise") => {
                    let eh = ValueID(env.new_variable());
                    let old_extent = ValueID(env.new_variable());
                    let k0 = ContinuationID(env.new_variable());
                    let k1 = ContinuationID(env.new_variable());
                    *self = LetContinuation {
                        id: k0,
                        kf: (vec![eh, old_extent], None),
                        k: Box::new(LetContinuation {
                            id: k1,
                            kf: (vec![], None),
                            k: Box::new(Apply {
                                operator: Atom::Variable(eh),
                                operands: std::mem::take(operands),
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
                    };
                    false
                },
                Atom::CorePrimitive("raise-continuable") => {
                    let eh = ValueID(env.new_variable());
                    let k0 = ContinuationID(env.new_variable());
                    *self = LetContinuation {
                        id: k0,
                        kf: (vec![eh], None), // this silently trims the arity
                        k: Box::new(Apply {
                            operator: Atom::Variable(eh),
                            operands: std::mem::take(operands),
                            k: *k,
                        }),
                        body: Box::new(Apply {
                            operator: Atom::CorePrimitive("__find_exception_handler"),
                            operands: (vec![], None),
                            k: Continuation::To(k0),
                        }),
                    };
                    false
                },
                Atom::CorePrimitive("with-exception-handler") => {
                    let dynamic_extent = ValueID(env.new_variable());
                    let handler = ValueID(env.new_variable());
                    let thunk = ValueID(env.new_variable());
                    let k0 = ContinuationID(env.new_variable());
                    let k1 = ContinuationID(env.new_variable());
                    let k2 = ContinuationID(env.new_variable());
                    *self = LetContinuation {
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
                                    k: *k,
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
                        body: Box::new(Continue { k: Continuation::To(k0), values: std::mem::take(operands) }),
                    };
                    false
                },
                Atom::CorePrimitive("make-parameter") => {
                    let init = ValueID(env.new_variable());
                    let parameter = ValueID(env.new_variable());
                    let present = ValueID(env.new_variable());
                    let value = ValueID(env.new_variable());
                    let k0 = ContinuationID(env.new_variable());
                    let k1 = ContinuationID(env.new_variable());
                    let bound_k = ContinuationID(env.new_variable());
                    *self = LetContinuation {
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
                                k: *k,
                                values: (vec![Atom::Variable(parameter)], None)
                            }),
                            pure: true,
                        }),
                        body: Box::new(Continue { k: Continuation::To(k0), values: std::mem::take(operands) }),
                    };
                    false
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
                    *self = LetContinuation {
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
                                    k: Box::new(Continue { k: *k, values: (vec![], Some(Atom::Variable(body_value))) }),
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
                        body: Box::new(Continue { k: Continuation::To(k0), values: std::mem::take(operands) }),
                    };
                    false
                },
                _ if operands.0.iter().all(|a| matches!(a, Atom::Literal(_))) && operands.1.is_none() => match *operator {
                    _ => true,
                },
                _ => true,
            },
            Branch { test, consequent, alternate } => match test {
                Atom::Literal(LiteralC::Boolean(false)) => { *self = std::mem::take(alternate); false },
                Atom::Literal(_) | Atom::Continuation(_) | Atom::CorePrimitive(_) => { *self = std::mem::take(consequent); false },
                _ => true,
            },
            Assign { vars, k, .. } if vars.0.is_empty() && vars.1.is_none() => {
                *self = Continue { k: *k, values: (vec![], None) };
                false
            },
            _ => true,
        }
    }

    pub fn hoist_lambdas(mut self, k: Continuation) -> Vec<ClosedLambda> {
        let mut lambdas = self.visit_multi(Vec::new(), |acc, e| {
            if let Expression::LetLambda { id, val, body, .. } = e {
                let closure = ClosedLambda::from(std::mem::take(val));
                let index = acc.len();
                *e = Expression::LetClosure {
                    id: *id,
                    index,
                    closed_env: closure.closed_env.clone(),
                    body: std::mem::take(body),
                };
                acc.push(closure);
            }
            true
        }).0;

        lambdas.push(ClosedLambda {
            closed_env: Vec::new(),
            formals: (vec![], None),
            k,
            body: Box::new(self),
        });
        lambdas
    }
}

pub fn transform(e: syntax::Expression, k: Continuation, env: &crate::Environment) -> Expression {
    let mut e = Expression::new(e, k, env);
    e.optimize(&env);
    e
}
