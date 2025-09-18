use crate::syntax;
use crate::syntax::{LiteralC, LiteralD};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)] pub struct ValueID(pub usize);
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
    /// Variables that need to be captured as part of the closure's environment.
    closed_env: Vec<ValueID>,
    /// An additional parameter: the continuation to invoke with the return value.
    k: ContinuationID,
    body: Box<Expression>,
}

#[derive(Debug, Clone, Default)]
pub enum Continuation {
    Variable(ContinuationID),
    Implicit { formals: (Vec<ValueID>, Option<ValueID>), body: Box<Expression> },
    #[default] Halt,
    Abort,
}

#[derive(Debug, Clone, Default)]
pub enum Expression {
    /// Identical to `body`, but with the literal `val` bound to the variable index `var`.
    /// `pure` if the only use of `val` is within `body`.
    LetLiteral { var: ValueID, val: LiteralD, body: Box<Expression>, pure: bool },
    /// Identical to `body`, but with the lambda `val` bound to the variable index `var`.
    /// `pure` if the only use of `val` is within `body`.
    LetLambda { var: ValueID, val: Lambda, body: Box<Expression>, pure: bool },
    /// Identical to `body`, but with the continuation `cont` bound to the variable index `var`.
    LetContinuation { var: ContinuationID, k: Continuation, body: Box<Expression> },
    /// Calls the lambda or primitive identified by `operator` with parameters identified by `operands`.
    /// Invokes the continuation `k` with the appropriate return values upon success.
    Apply { operator: Atom, operands: (Vec<Atom>, Option<Atom>), k: Continuation },
    /// Identical to `consequent` unless `test` evaluates to false; then, `alternate`.
    Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
    /// Invokes `k` with `values`.
    Continue { k: Continuation, values: (Vec<Atom>, Option<Atom>) },
    /// Invokes `k` with no values after binding elements of `val`, in order, to elements of `var`, any leftovers in the tail.
    Assign { vars: (Vec<ValueID>, Option<ValueID>), values: (Vec<Atom>, Option<Atom>), k: Continuation },
    /// Represents finality in execution. Does not continue.
    #[default] Halt,
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
    fn rename_continues(self, from: ContinuationID, to: ContinuationID) -> Self {
        use Continuation::*;
        match self {
            Variable(i) => Variable(if i == from { to } else { i }),
            Implicit { formals, body } => Implicit { formals, body: Box::new(body.rename_continues(from, to)) },
            Halt | Abort => self,
        }
    }

    fn complexity(&self) -> usize {
        match self {
            Continuation::Implicit { body, .. } => body.complexity(),
            _ => 0,
        }
    }
    fn uses_variable(&self, v: ValueID) -> usize {
        match self {
            Continuation::Implicit { body, .. } => body.uses_variable(v),
            _ => 0,
        }
    }
}

impl Expression {
    fn complexity(&self) -> usize {
        use Expression::*;
        match self {
            LetLiteral { body, .. } => body.complexity(),
            LetLambda { val, body, .. } => val.body.complexity() + body.complexity(),
            LetContinuation { k, body, .. } => k.complexity() + body.complexity(),
            Branch { consequent, alternate, .. } => consequent.complexity() + alternate.complexity(),
            Apply { k, .. } | Continue { k, .. } | Assign { k, .. } => k.complexity(),
            Halt => 0,
        }
    }

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
                Continue { k, .. } | Assign { k, .. }=> {
                    if let Continuation::Implicit { body: k_body, .. } = k {
                        visit_children(k_body.as_mut(), a, f) && f(k_body.as_mut(), a)
                    } else {
                        true
                    }
                },
                Apply { .. } | Halt => true,
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
                Apply { .. } | Halt => true,
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
            LetLiteral { var, val, body, pure } => LetLiteral {
                var,
                val,
                body: Box::new(body.rename(from, to)),
                pure,
            },
            LetLambda { var, val, body, pure } => LetLambda {
                var,
                val: Lambda {
                    formals: val.formals,
                    closed_env: val.closed_env,
                    k: val.k,
                    body: Box::new(val.body.rename(from, to)),
                },
                body: Box::new(body.rename(from, to)),
                pure,
            },
            LetContinuation { var, k, body } => LetContinuation {
                var,
                k: k.rename(from, to),
                body: Box::new(body.rename(from, to)),
            },
            Apply { operator, operands, k } => Apply {
                operator: map_atom(operator),
                operands: (operands.0.into_iter().map(map_atom).collect(), operands.1.map(map_atom)),
                k,
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
            Halt => Halt,
        }
    }

    fn rename_continues(self, from: ContinuationID, to: ContinuationID) -> Self {
        use Expression::*;
        let map_atom = |a| if let Atom::Continuation(i) = a { Atom::Continuation(if i == from { to } else { i }) } else { a };
        match self {
            LetLiteral { var, val, body, pure } => LetLiteral {
                var,
                val,
                body: Box::new(body.rename_continues(from, to)),
                pure,
            },
            LetLambda { var, val, body, pure } => LetLambda {
                var,
                val: Lambda {
                    formals: val.formals,
                    closed_env: val.closed_env,
                    k: if val.k == from { to } else { val.k },
                    body: Box::new(val.body.rename_continues(from, to)),
                },
                body: Box::new(body.rename_continues(from, to)),
                pure,
            },
            LetContinuation { var, k, body } => LetContinuation {
                var,
                k: k.rename_continues(from, to),
                body: Box::new(body.rename_continues(from, to)),
            },
            Apply { operator, operands, k } => Apply {
                operator: map_atom(operator),
                operands: (operands.0.into_iter().map(map_atom).collect(), operands.1.map(map_atom)),
                k: match k {
                    Continuation::Variable(i) if i == from => Continuation::Variable(to),
                    _ => k,
                },
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
            Halt => Halt,
        }
    }

    fn uses_variable(&self, v: ValueID) -> usize {
        use Expression::*;
        match self {
            LetLiteral { body, .. } => body.uses_variable(v),
            LetLambda { val, body, .. } => val.body.uses_variable(v) + body.uses_variable(v),
            LetContinuation { k, body, .. } => k.uses_variable(v) + body.uses_variable(v),
            Apply { operator, operands, k } => std::iter::once(operator).chain(operands.0.iter()).chain(operands.1.iter()).filter(|&&atom| atom == Atom::Variable(v)).count() + k.uses_variable(v),
            Branch { test, consequent, alternate } => consequent.uses_variable(v) + alternate.uses_variable(v) + (if *test == Atom::Variable(v) { 1 } else { 0 }),
            Continue { k, values } | Assign { k, values, .. } => k.uses_variable(v) + values.0.iter().chain(values.1.iter()).filter(|&&atom| atom == Atom::Variable(v)).count(),
            Halt => 0,
        }
    }
    fn uses_continuation(&self, k_id: ContinuationID) -> bool {
        use Expression::*;
        match self {
            LetLiteral { body, .. } | LetLambda { body, .. } | LetContinuation { body, .. } => body.uses_continuation(k_id),
            Apply { operator, operands, k} => std::iter::once(operator).chain(operands.0.iter()).chain(operands.1.iter()).any(|&atom| atom == Atom::Continuation(k_id)) || match k {
                Continuation::Variable(i) => *i == k_id,
                Continuation::Implicit { body, .. } => body.uses_continuation(k_id),
                _ => false,
            },
            Branch { test, consequent, alternate } => *test == Atom::Continuation(k_id) || consequent.uses_continuation(k_id) || alternate.uses_continuation(k_id),
            Continue { k, values } | Assign { values, k, .. } => values.0.iter().chain(values.1.iter()).any(|&atom| atom == Atom::Continuation(k_id)) || match k {
                Continuation::Variable(i) => *i == k_id,
                Continuation::Implicit { body, .. } => body.uses_continuation(k_id),
                _ => false,
            },
            Halt => false,
        }
    }

    fn variables(&self) -> std::collections::BTreeSet<ValueID> {
        let atom2var = |atom: &Atom| if let Atom::Variable(i) = atom { Some(*i) } else { None };
        let mut uses = std::collections::BTreeSet::new();
        let mut intros = std::collections::BTreeSet::new();
        let mut queue = std::iter::once(self).collect::<std::collections::VecDeque<_>>();
        while let Some(e) = queue.pop_front() {
            use Expression::*;
            match e {
                LetLiteral { var, body, .. } => {
                    intros.insert(*var);
                    queue.push_back(body.as_ref());
                },
                LetLambda { var, val, body, .. } => {
                    intros.insert(*var);
                    intros.extend(val.formals.0.iter().chain(val.formals.1.iter()));
                    queue.push_back(body.as_ref());
                },
                LetContinuation { k, body, .. } => {
                    if let Continuation::Implicit { formals, body } = k {
                        intros.extend(formals.0.iter().chain(formals.1.iter()));
                        queue.push_back(body.as_ref());
                    }
                    queue.push_back(body.as_ref());
                },
                Apply { operator, operands, k } => {
                    uses.extend(std::iter::once(operator)
                        .chain(operands.0.iter())
                        .chain(operands.1.iter())
                        .filter_map(atom2var));
                    if let Continuation::Implicit { formals, body } = k {
                        intros.extend(formals.0.iter().chain(formals.1.iter()));
                        queue.push_back(body.as_ref());
                    }
                },
                Branch { test, consequent, alternate } => {
                    if let Atom::Variable(test) = test {
                        uses.insert(*test);
                    }
                    queue.push_back(consequent.as_ref());
                    queue.push_back(alternate.as_ref());
                },
                Continue { k, values } => {
                    if let Continuation::Implicit { formals, body } = k {
                        intros.extend(formals.0.iter().chain(formals.1.iter()));
                        queue.push_back(body.as_ref());
                    }
                    uses.extend(values.0.iter().chain(values.1.iter()).filter_map(atom2var));
                },
                Assign { vars, values, k } => {
                    intros.extend(vars.0.iter().chain(vars.1.iter())); // The value is being introduced to this variable here ...
                    uses.extend(values.0.iter().chain(values.1.iter()).filter_map(atom2var));
                    if let Continuation::Implicit { formals, body } = k {
                        intros.extend(formals.0.iter().chain(formals.1.iter()));
                        queue.push_back(body.as_ref());
                    }
                },
                Halt => (),
            }
        }

        uses.difference(&intros).copied().collect()
    }
    fn convert_closures(&mut self) {
        self.visit_mut(|e| {
            if let Expression::LetLambda { val: Lambda { formals, closed_env, body, .. }, .. } = e {
                *closed_env = body.variables()
                    .difference(&formals.0.iter().chain(formals.1.iter()).copied().collect())
                    .copied().collect();
            }
            true
        });
    }
}

impl crate::Environment {
    /// Atomizes an expression `e` so it can be used in other expressions, with `kf` the failure continuation.
    /// If the expression is not atomic, its value is bound to a new variable.
    /// Returns an expression that is the result of using the atom (with `then`), whether directly, or after binding it to a variable.
    fn atomize(&self, e: syntax::Expression, then: impl FnOnce(Atom) -> Expression) -> Expression {
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
                    body: Box::new(self.cps_transform(e, k)),
                }
            }
        }
    }

    /// Atomizes a bunch of expressions `ee`, just like `atomize`. `then` is invoked with all the atoms in `ee`.
    fn atomize_many(&self, ee: Vec<syntax::Expression>, then: impl FnOnce(Vec<Atom>) -> Expression) -> Expression {
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
                        body: Box::new(self.cps_transform(e, kx)),
                    }
                }
            })
    }

    /// Transforms `e` into continuation-passing style and invokes `k` on it.
    fn cps_transform(&self, e: syntax::Expression, k: ContinuationID) -> Expression {
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
                    pure: true,
                } },
            syntax::Expression::ProcedureCall { operator, operands } => {
                self.atomize(*operator, move |operator|
                    self.atomize_many(operands, move |operands| Expression::Apply {
                        operator,
                        operands: (operands, None),
                        k: Continuation::Variable(k),
                    }))
            }
            syntax::Expression::Lambda { formals, body } => {
                let x = ValueID(self.new_variable());
                let kx = ContinuationID(self.new_variable());
                let body = self.transform_block(body, kx);
                Expression::LetLambda {
                    var: x,
                    val: Lambda {
                        formals: (formals.0.iter().map(ValueID::from).collect(), formals.1.map(|e| ValueID::from(e.as_ref()))),
                        closed_env: Vec::new(),
                        k: kx,
                        body: Box::new(body),
                    },
                    body: Box::new(Expression::Continue {
                        k: Continuation::Variable(k),
                        values: (vec![Atom::Variable(x)], None),
                    }),
                    pure: true,
                }
            }
            syntax::Expression::Conditional { test, consequent, alternate } => {
                self.atomize(*test, move |test| Expression::Branch {
                    test,
                    consequent: Box::new(self.cps_transform(*consequent, k)),
                    alternate: Box::new(match alternate {
                        None => Expression::Continue {
                            k: Continuation::Variable(k),
                            values: (vec![Atom::Literal(LiteralC::Boolean(false))], None),
                        },
                        Some(alternate) => self.cps_transform(*alternate, k),
                    }),
                })
            }
            syntax::Expression::Assignment { ids, value } => {
                let x = ValueID(self.new_variable());
                let kx = ContinuationID(self.new_variable());
                Expression::LetContinuation {
                    var: kx,
                    k: Continuation::Implicit {
                        formals: (vec![], Some(x)),
                        body: Box::new(Expression::Assign {
                            vars: (ids.0.iter().map(|e| ValueID::from(e)).collect(), ids.1.map(|e| ValueID::from(e.as_ref()))),
                            values: (vec![], Some(Atom::Variable(x))),
                            k: Continuation::Variable(k),
                        }),
                    },
                    body: Box::new(self.cps_transform(*value, kx)),
                }
            },
            syntax::Expression::Definition { ids, value } => {
                let x = ValueID(self.new_variable());
                let kx = ContinuationID(self.new_variable());
                Expression::LetContinuation {
                    var: kx,
                    k: Continuation::Implicit {
                        formals: (vec![], Some(x)),
                        body: Box::new(Expression::Assign {
                            vars: (ids.0.iter().map(|e| ValueID::from(e)).collect(), ids.1.map(|e| ValueID::from(e.as_ref()))),
                            values: (vec![], Some(Atom::Variable(x))),
                            k: Continuation::Variable(k),
                        }),
                    },
                    body: Box::new(self.cps_transform(*value, kx)),
                }
            }
            syntax::Expression::Block { body } => self.transform_block(body, k),
        }
    }
    fn transform_block(&self, e: Vec<syntax::Expression>, k: ContinuationID) -> Expression {
        let mut ee = e.into_iter().rev();
        if let Some(tail) = ee.next() {
            ee.fold(self.cps_transform(tail, k), |acc, e| {
                let kx = ContinuationID(self.new_variable());
                Expression::LetContinuation {
                    var: kx,
                    k: Continuation::Implicit {
                        formals: (vec![], Some(ValueID(self.new_variable()))),
                        body: Box::new(acc),
                    },
                    body: Box::new(self.cps_transform(e, kx)),
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
                    LetLiteral { var, body, pure, .. } => {
                        match (*pure, body.uses_variable(*var)) {
                            (true, 0) => {
                                *e = std::mem::take(body);
                                false
                            },
                            (true, 1) => body.visit_mut(|e| {
                                if let Assign { vars, values, .. } = e {
                                    if let Some(index) = values.0.iter().enumerate().find_map(|(i, &v)| if v == Atom::Variable(*var) { Some(i) } else { None }) {
                                        values.0.swap_remove(index);
                                        *var = vars.0.swap_remove(index);
                                        *pure = false;
                                        return false;
                                    }
                                }
                                true
                            }),
                            _ => true,
                        }
                    },
                    LetLambda { var, val, body, pure } => {
                        let uses = body.uses_variable(*var);
                        if *pure && uses == 0 {
                            *e = std::mem::take(body);
                            return false;
                        }

                        let mut beta_expanded = false;
                        body.visit_mut(|e| {
                            if let Apply { operator: Atom::Variable(operator), operands, k } = e {
                                if (val.body.complexity() < 6 || uses == 1) && operator == var {
                                    let mut xfm = |k: ContinuationID| std::mem::take(val.body.as_mut())
                                        .rename_continues(val.k, k)
                                        .rename(&val.formals, &operands);
                                    *e = match k {
                                        Continuation::Variable(k) => xfm(*k),
                                        _ => {
                                            let k0 = ContinuationID(self.new_variable());
                                            LetContinuation {
                                                var: k0,
                                                k: std::mem::take(k),
                                                body: Box::new(xfm(k0)),
                                            }
                                        },
                                    };
                                    beta_expanded = true;
                                    return uses > 1;
                                }
                            }
                            true
                        });
                        if beta_expanded {
                            if *pure {
                                *e = std::mem::take(body);
                            }
                            return false;
                        }

                        if *pure && uses == 1 {
                            if body.visit_mut(|e| {
                                if let Assign { vars, values, .. } = e {
                                    if let Some(index) = values.0.iter().enumerate().find_map(|(i, &v)| if v == Atom::Variable(*var) { Some(i) } else { None }) {
                                        values.0.swap_remove(index);
                                        *var = vars.0.swap_remove(index);
                                        *pure = false;
                                        return false;
                                    }
                                }
                                true
                            }) == false {
                                return false;
                            }
                        }

                        true
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
                                        Continuation::Halt | Continuation::Abort => Continue {
                                            k: k.clone(),
                                            values: std::mem::take(values)
                                        },
                                    };
                                }
                            }
                            true
                        });

                        if body.uses_continuation(*var) == false {
                            *e = std::mem::take(body);
                            false
                        } else {
                            true
                        }
                    }
                    Apply { operator, operands, k } => {
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
                                        k: std::mem::take(k),
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
                                                k: std::mem::take(k),
                                            }),
                                        },
                                        values: std::mem::take(operands),
                                    };
                                }
                                false
                            },
                            Atom::CorePrimitive("call/cc" | "call-with-current-continuation") => {
                                let mut ccc = |operands: &[Atom]| match k {
                                    Continuation::Variable(k) => Apply {
                                        operator: operands[0],
                                        operands: (vec![Atom::Continuation(*k)], None),
                                        k: Continuation::Variable(*k),
                                    },
                                    _ => {
                                        let k0 = ContinuationID(self.new_variable());
                                        LetContinuation {
                                            var: k0,
                                            k: std::mem::take(k),
                                            body: Box::new(Apply {
                                                operator: operands[0],
                                                operands: (vec![Atom::Continuation(k0)], None),
                                                k: Continuation::Variable(k0),
                                            }),
                                        }
                                    },
                                };
                                if operands.0.len() >= 1 {
                                    *e = ccc(&operands.0);
                                } else {
                                    let x0 = ValueID(self.new_variable());
                                    let body = ccc(&vec![Atom::Variable(x0)]);
                                    *e = Continue {
                                        k: Continuation::Implicit {
                                            formals: (vec![x0], None),
                                            body: Box::new(body),
                                        },
                                        values: std::mem::take(operands),
                                    };
                                }
                                false
                            },
                            Atom::CorePrimitive("call-with-values") => {
                                let x0 = ValueID(self.new_variable());
                                let mut cwv = |operands: &[Atom]| Apply {
                                    operator: operands[0],
                                    operands: (vec![], None),
                                    k: Continuation::Implicit {
                                        formals: (vec![], Some(x0)),
                                        body: Box::new(Apply {
                                            operator: operands[1],
                                            operands: (vec![], Some(Atom::Variable(x0))),
                                            k: std::mem::take(k),
                                        }),
                                    },
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
                                let mut dw = |operands: &[Atom]| Apply {
                                    operator: operands[0],
                                    operands: (vec![], None),
                                    k: Continuation::Implicit {
                                        formals: (vec![], None),
                                        body: Box::new(Apply {
                                            operator: Atom::CorePrimitive("__push_dynamic_frame"),
                                            operands: (vec![operands[0], operands[2]], None),
                                            k: Continuation::Implicit {
                                                formals: (vec![], None),
                                                body: Box::new(Apply {
                                                    operator: operands[1],
                                                    operands: (vec![], None),
                                                    k: std::mem::take(k),
                                                })
                                            },
                                        }),
                                    },
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
                                    k: std::mem::take(k),
                                    values: std::mem::take(operands),
                                };
                                false
                            },
                            Atom::CorePrimitive("raise" | "raise-continuable") => {
                                let eh = ValueID(self.new_variable());
                                let mut raise = |continuable| Apply {
                                    operator: Atom::CorePrimitive("__peek_exception_handler"),
                                    operands: (vec![], None),
                                    k: Continuation::Implicit {
                                        formals: (vec![eh], None),
                                        body: Box::new(Apply {
                                            operator: Atom::Variable(eh),
                                            operands: (match operands.0.first() {
                                                Some(o) => vec![*o],
                                                None => vec![],
                                            }, None),
                                            k: if continuable { std::mem::take(k) } else { Continuation::Abort },
                                        })
                                    },
                                };

                                *e = match operator {
                                    Atom::CorePrimitive("raise") => raise(false),
                                    Atom::CorePrimitive("raise-continuable") => raise(true),
                                    _ => unreachable!(),
                                };
                                false
                            },
                            Atom::CorePrimitive("with-exception-handler") => {
                                let mut weh = |operands: &[Atom]| Apply {
                                    operator: Atom::CorePrimitive("__push_exception_handler"),
                                    operands: (vec![operands[0]], None),
                                    k: Continuation::Implicit {
                                        formals: (vec![], None),
                                        body: Box::new(Apply {
                                            operator: operands[1],
                                            operands: (vec![], None),
                                            k: Continuation::Implicit {
                                                formals: (vec![], None),
                                                body: Box::new(Apply {
                                                    operator: Atom::CorePrimitive("__pop_exception_handler"),
                                                    operands: (vec![operands[0]], None),
                                                    k: std::mem::take(k),
                                                }),
                                            },
                                        }),
                                    },
                                };

                                if operands.0.len() >= 2 {
                                    *e = weh(&operands.0);
                                } else {
                                    let formals = (0..2).map(|_| ValueID(self.new_variable())).collect::<Vec<_>>();
                                    *e = Continue {
                                        k: Continuation::Implicit {
                                            formals: (formals.clone(), None),
                                            body: Box::new(weh(&formals.into_iter().map(Atom::Variable).collect::<Vec<_>>())),
                                        },
                                        values: std::mem::take(operands),
                                    };
                                }
                                false
                            }
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
                    },
                    Assign { vars, k, .. } => {
                        if vars.0.is_empty() && vars.1.is_none() {
                            *e = Continue { k: std::mem::take(k), values: (vec![], None) };
                            false
                        } else {
                            true
                        }
                    }
                    _ => true,
                }
            }) { break; }
        }

        e
    }
}

pub fn transform(e: syntax::Expression, k: ContinuationID, env: &crate::Environment) -> Expression {
    let e = env.cps_transform(e, k);
    let mut e = env.optimize_cps(e);
    e.convert_closures();
    e
}
