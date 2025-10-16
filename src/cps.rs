use crate::core::Arity;
use crate::syntax;
use crate::syntax::{LiteralC, LiteralD};
use std::collections::{HashSet, HashMap};
use std::ops::ControlFlow;
const BETA_EXPANSION_COMPLEXITY_THRESHOLD: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueID(pub usize);
impl ValueID {
    pub fn invalid() -> Self {
        Self(0)
    }
}
impl From<&syntax::Expression> for ValueID {
    fn from(value: &syntax::Expression) -> Self {
        if let syntax::Expression::Variable(i) = value {
            ValueID(*i)
        } else {
            panic!()
        }
    }
}
impl<T> From<&(Vec<T>, Option<ValueID>)> for Arity {
    fn from(value: &(Vec<T>, Option<ValueID>)) -> Self {
        use Arity::*;
        let count = value.0.len();
        match value.1 {
            None => Exact(count),
            Some(_) => AtLeast(count),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContinuationID(pub usize);
impl From<ContinuationID> for ValueID {
    fn from(value: ContinuationID) -> Self {
        Self(value.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum ContinuationRef {
    #[default] Escape,
    Abort,
    To(ContinuationID),
}

#[derive(Debug, Clone)]
pub struct ContinuationDef {
    pub(crate) formals: (Vec<ValueID>, Option<ValueID>),
    pub(crate) body: Box<Expression>,
}

#[derive(Debug, Clone)]
pub enum Continuation {
    Ref(ContinuationRef),
    Def(ContinuationDef),
}
impl Default for Continuation {
    fn default() -> Self {
        Continuation::Ref(Default::default())
    }
}
impl From<ContinuationID> for Continuation {
    fn from(value: ContinuationID) -> Self {
        Continuation::Ref(ContinuationRef::To(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Atom {
    Literal(LiteralC),
    Variable(ValueID),
    CorePrimitive(&'static str),
}
impl From<&Vec<Atom>> for Arity {
    fn from(value: &Vec<Atom>) -> Self {
        Arity::Exact(value.len())
    }
}

#[derive(Debug, Clone)]
pub struct Lambda {
    /// The defined parameters of this function.
    pub(crate) formals: (Vec<ValueID>, Option<ValueID>),
    /// The continuation to invoke with the return value.
    pub(crate) escape: ContinuationRef,
    pub(crate) body: Box<Expression>,
}

#[derive(Debug, Clone, Default)]
pub enum Expression {
    /// Identical to `body`, but with the literal `val` bound to the variable index `id`.
    LetLiteral { id: ValueID, val: LiteralD, body: Box<Expression> },
    /// Identical to `body`, but with the lambda `val` bound to the variable index `id`.
    LetLambda { id: ValueID, val: Lambda, body: Box<Expression> },
    /// Identical to `body`, but with a continuation defined by `k` bound to the variable index `id`.
    LetContinuation { id: ContinuationID, k: ContinuationDef, body: Box<Expression> },
    /// Calls the lambda or primitive identified by `operator` with parameters identified by `operands`.
    /// Invokes the continuation `k` with `kontract` values returned from the call to `operator`.
    Apply { operator: Atom, operands: (Vec<Atom>, Option<ValueID>), kontract: Arity, k: Continuation },
    /// Identical to `consequent` unless `test` evaluates to false; then, `alternate`.
    Branch { test: Atom, consequent: Box<Expression>, alternate: Box<Expression> },
    /// Invokes `k` with `values`.
    Continue { k: Continuation, values: (Vec<Atom>, Option<ValueID>) },
    /// Invokes `k` after binding `values` and any varargs into `vars`.
    Assign { vars: (Vec<ValueID>, Option<ValueID>), values: (Vec<Atom>, Option<ValueID>), k: Continuation },
    #[default] TakenOut,
}

#[derive(Debug, Clone)]
pub enum Error {
    InvalidApplication { operator: Atom },
    InvalidApplicationArity { operator: Atom, expected: Arity, actual: Arity },
    InvalidContinuationArity { expected: Arity, actual: Arity },
}

type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
struct Environment<'stx> {
    stx_env: &'stx syntax::Environment,
    visible_bindings: HashSet<ValueID>,
}
impl<'stx> From<&'stx syntax::Environment> for Environment<'stx> {
    fn from(stx_env: &'stx syntax::Environment) -> Self {
        Self {
            stx_env,
            visible_bindings: stx_env.bound_variables().map(ValueID).collect(),
        }
    }
}

impl Expression {
    fn visit<T>(&self, init: T, mut f: impl FnMut(T, &Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
        fn visit_children<T>(expr: &Expression, init: T, f: &mut impl FnMut(T, &Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
            use Expression::*;
            match expr {
                LetLiteral { body, .. } => {
                    let r = visit_children(body.as_ref(), init, f)?;
                    f(r, body.as_ref())
                },
                LetLambda { val, body, .. } => {
                    let r = visit_children(val.body.as_ref(), init, f)?;
                    let r = f(r, val.body.as_ref())?;
                    let r = visit_children(body.as_ref(), r, f)?;
                    f(r, body.as_ref())
                },
                LetContinuation { k, body, .. } => {
                    let r = visit_children(k.body.as_ref(), init, f)?;
                    let r = f(r, k.body.as_ref())?;
                    let r = visit_children(body.as_ref(), r, f)?;
                    f(r, body.as_ref())
                },
                Branch { consequent, alternate, .. } => {
                    let r = visit_children(consequent.as_ref(), init, f)?;
                    let r = f(r, consequent.as_ref())?;
                    let r = visit_children(alternate.as_ref(), r, f)?;
                    f(r, alternate.as_ref())
                },
                Apply { k: Continuation::Def(k), .. } | Continue { k: Continuation::Def(k), .. } | Assign { k: Continuation::Def(k), .. } => {
                    let r = visit_children(k.body.as_ref(), init, f)?;
                    f(r, k.body.as_ref())
                },
                Apply { .. } | Continue { .. } | Assign { .. } | TakenOut => ControlFlow::Continue(init),
            }
        }

        let r = visit_children(self, init, &mut f)?;
        f(r, self)
    }
    fn visit_mut<T>(&mut self, init: T, mut f: impl FnMut(T, &mut Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
        fn visit_children<T>(expr: &mut Expression, init: T, f: &mut impl FnMut(T, &mut Expression) -> ControlFlow<T, T>) -> ControlFlow<T, T> {
            use Expression::*;
            match expr {
                LetLiteral { body, .. } => {
                    let r = visit_children(body.as_mut(), init, f)?;
                    f(r, body.as_mut())
                },
                LetLambda { val, body, .. } => {
                    let r = visit_children(val.body.as_mut(), init, f)?;
                    let r = f(r, val.body.as_mut())?;
                    let r = visit_children(body.as_mut(), r, f)?;
                    f(r, body.as_mut())
                },
                LetContinuation { k, body, .. } => {
                    let r = visit_children(&mut k.body, init, f)?;
                    let r = f(r, &mut k.body)?;
                    let r = visit_children(body.as_mut(), r, f)?;
                    f(r, body.as_mut())
                },
                Branch { consequent, alternate, .. } => {
                    let r = visit_children(consequent.as_mut(), init, f)?;
                    let r = f(r, consequent.as_mut())?;
                    let r = visit_children(alternate.as_mut(), r, f)?;
                    f(r, alternate.as_mut())
                },
                Apply { k: Continuation::Def(k), .. } | Continue { k: Continuation::Def(k), .. } | Assign { k: Continuation::Def(k), .. } => {
                    let r = visit_children(k.body.as_mut(), init, f)?;
                    f(r, k.body.as_mut())
                },
                Apply { .. } | Continue { .. } | Assign { .. } | TakenOut => ControlFlow::Continue(init),
            }
        }

        let r = visit_children(self, init, &mut f)?;
        f(r, self)
    }

    fn complexity(&self) -> usize {
        self.visit(0, |count, _| ControlFlow::Continue(count + 1))
            .continue_value().unwrap()
    }
    fn uses_variable(&self, id: ValueID) -> usize {
        self.visit(0, |count, e| {
            use Expression::*;
            ControlFlow::Continue(count + match e {
                Apply { operator, operands, .. } => std::iter::once(operator).chain(operands.0.iter()).filter(|&&a| a == Atom::Variable(id)).count() + (if operands.1.is_some_and(|i| i == id) { 1 } else { 0 }),
                Branch { test, .. } => if *test == Atom::Variable(id) { 1 } else { 0 },
                Continue { values, .. } | Assign { values, .. } => values.0.iter().filter(|&&a| a == Atom::Variable(id)).count() + (if values.1.is_some_and(|i| i == id) { 1 } else { 0 }),
                _ => 0,
            })
        }).continue_value().unwrap()
    }

    fn check_arity(&mut self) -> Result<()> {
        use Expression::*;
        let mut in_lambdas = HashMap::new(); // The arity of inputs to each lambda
        let mut mid_lambdas = HashMap::new();
        let mut continues = HashMap::new(); // The arity of inputs to each non-inline continuation

        self.visit((), |_, e| {
            if let LetLambda { id, val: Lambda { formals, escape, body }, .. } = e {
                in_lambdas.insert(*id, Arity::from(formals));
                mid_lambdas.insert(*id, body.visit((None, HashSet::new()), |(explicits, mut applies), e| {
                    if let Continue { k: Continuation::Ref(k), values } = e {
                        if k == escape {
                            return match explicits {
                                None => ControlFlow::Continue((Some(Arity::from(values)), applies)),
                                Some(explicits) => ControlFlow::Continue((Some(explicits.join(Arity::from(values))), applies)),
                            };
                        }
                    } else if let Apply { operator, k: Continuation::Ref(k), .. } = e {
                        if k == escape {
                            if let Atom::CorePrimitive(operator) = operator {
                                let out_arity = crate::core::PRIMITIVES.get(operator).unwrap().1;
                                return match explicits {
                                    None => ControlFlow::Continue((Some(out_arity), applies)),
                                    Some(explicits) => ControlFlow::Continue((Some(explicits.join(out_arity)), applies)),
                                };
                            } else if let Atom::Variable(x) = operator {
                                applies.insert(*x);
                            }
                        }
                    }
                    ControlFlow::Continue((explicits, applies))
                }).continue_value().unwrap());
            } else if let LetContinuation { id, k: ContinuationDef { formals, .. }, .. } = e {
                continues.insert(*id, Arity::from(formals));
            }
            ControlFlow::Continue(())
        }).continue_value().unwrap();

        let mut out_lambdas = HashMap::new(); // The arity of outputs from each lambda
        let mut mid_lambdas = mid_lambdas.into_iter()
            .collect::<Vec<_>>();
        mid_lambdas.sort_by_key(|(_, (_, deps))| deps.len());
        for (id, (base_arity, deps)) in mid_lambdas.into_iter() {
            let dep_arity = deps.iter()
                .map(|x| out_lambdas.get(x).copied().unwrap_or(Arity::Unknown))
                .reduce(|acc, e| acc.join(e));
            out_lambdas.insert(id, match (base_arity, dep_arity) {
                (None, None) => Arity::Unknown,
                (None, Some(x)) | (Some(x), None) => x,
                (Some(a), Some(b)) => a.join(b),
            });
        }

        let k_arity = |k: &Continuation| match k {
            Continuation::Ref(ContinuationRef::To(k)) => continues.get(k).copied(),
            Continuation::Ref(_) => Some(Arity::Unknown),
            Continuation::Def(ContinuationDef { formals, .. }) => Some(Arity::from(formals)),
        };
        match self.visit_mut(None, |_, e| {
            if let Apply { operator, operands, kontract, k } = e {
                match operator {
                    Atom::Literal(_) => return ControlFlow::Break(Some(Error::InvalidApplication { operator: *operator })),
                    Atom::Variable(x) => {
                        let (expected, actual) = (in_lambdas.get(x).copied().unwrap_or(Arity::Unknown), Arity::from(&*operands));
                        if !expected.compatible_with(actual) {
                            return ControlFlow::Break(Some(Error::InvalidApplicationArity { operator: *operator, expected, actual }));
                        }
                        if let Some(actual) = out_lambdas.get(x).copied() {
                            if let Some(expected) = k_arity(k) {
                                if !expected.compatible_with(actual) {
                                    return ControlFlow::Break(Some(Error::InvalidContinuationArity { expected, actual }));
                                }
                            }
                            *kontract = actual;
                        }
                    }
                    Atom::CorePrimitive(s) => {
                        let primitive_arity = crate::core::PRIMITIVES.get(s).copied().unwrap();
                        let (expected, actual) = (primitive_arity.0, Arity::from(&*operands));
                        if !expected.compatible_with(actual) {
                            return ControlFlow::Break(Some(Error::InvalidApplicationArity { operator: *operator, expected, actual }));
                        }
                        let actual = primitive_arity.1;
                        if actual != Arity::Unknown { // optimization of the core primitive will take care of this later
                            if let Some(expected) = k_arity(k) {
                                if !expected.compatible_with(actual) {
                                    return ControlFlow::Break(Some(Error::InvalidContinuationArity { expected, actual }));
                                }
                            }
                            *kontract = actual;
                        }
                    }
                }
            } else if let Continue { k, values } = e {
                if let Some(expected) = k_arity(k) {
                    let actual = Arity::from(&*values);
                    if !expected.compatible_with(actual) {
                        return ControlFlow::Break(Some(Error::InvalidContinuationArity { expected, actual }));
                    }
                }
            } else if let Assign { vars, values, k } = e {
                let expected = Arity::from(&*values);
                let actual = Arity::from(&*vars);
                if !expected.compatible_with(actual) {
                    return ControlFlow::Break(Some(Error::InvalidContinuationArity { expected, actual }));
                }
                if let Some(expected) = k_arity(k) {
                    let actual = Arity::Exact(0);
                    if !expected.compatible_with(actual) {
                        return ControlFlow::Break(Some(Error::InvalidContinuationArity { expected, actual }));
                    }
                }
            }
            ControlFlow::Continue(None)
        }) {
            ControlFlow::Break(Some(e)) => Err(e),
            _ => Ok(()),
        }
    }

    fn rename(&mut self, from: &(Vec<ValueID>, Option<ValueID>), to: &(Vec<Atom>, Option<ValueID>)) -> bool {
        assert!(from.0.len() <= to.0.len());
        let mapping = std::iter::zip(from.0.iter().copied(), to.0.iter().copied()).collect::<HashMap<_, _>>();
        let tail_mapping = (&to.0[from.0.len() ..], to.1);
        let map_atom = |a: &mut Atom| {
            if let Atom::Variable(i) = a {
                if let Some(&mapped) = mapping.get(i) {
                    *a = mapped;
                    return true;
                }
            };
            false
        };
        let map_atoms = |aa: &mut (Vec<Atom>, Option<ValueID>)| {
            let size_hint = aa.0.len();
            let (mut modified, mut new_0) = aa.0.iter_mut().fold((false, Vec::with_capacity(size_hint)), |(unmodified, mut dest), a| {
                if let Atom::Variable(i) = a {
                    if let Some(&mapped) = mapping.get(i) {
                        dest.push(mapped);
                        return (true, dest);
                    } else if from.1.is_some_and(|tail| *i == tail) {
                        dest.extend(tail_mapping.0);
                        dest.extend(tail_mapping.1.iter().copied().map(Atom::Variable)); // the runtime may treat this as a list.
                        return (true, dest);
                    }
                }
                dest.push(*a);
                (unmodified, dest)
            });
            let new_1 = if aa.1 == from.1 {
                modified = true;
                new_0.extend(tail_mapping.0);
                tail_mapping.1
            } else {
                aa.1
            };
            if modified {
                *aa = (new_0, new_1);
            }
            modified
        };

        self.visit_mut(false, |unchanged, e| {
            use Expression::*;
            ControlFlow::Continue(match e {
                Apply { operator, operands, .. } => map_atom(operator) | map_atoms(operands),
                Branch { test, .. } => map_atom(test),
                Continue { values, .. } | Assign { values, .. } => map_atoms(values),
                _ => unchanged,
            })
        }).continue_value().unwrap()
    }

    fn recontinue(&mut self, from: ContinuationRef, to: Continuation) -> bool {
        use Expression::*;
        let map_k = |k: &mut Continuation| match k {
            Continuation::Ref(kr) if *kr == from => {
                *k = to.clone();
                true
            },
            Continuation::Ref(_) => false,
            Continuation::Def(_) => false,
        };
        self.visit_mut(false, |modified, e| match e {
            Apply { k, .. } | Continue { k, .. } | Assign { k, .. } => ControlFlow::Continue(map_k(k) || modified),
            _ => ControlFlow::Continue(modified),
        }).continue_value().unwrap()
    }

    fn hoist_assignment(&mut self, id: &mut ValueID) -> bool {
        use Expression::*;
        let Assign { vars, values, .. } = self else { panic!() };
        if let Some(index) = values.0.iter().enumerate().find_map(|(i, &v)| if v == Atom::Variable(*id) { Some(i) } else { None }) {
            values.0.swap_remove(index);
            *id = vars.0.swap_remove(index);
            true
        } else {
            false
        }
    }
}

impl<'stx> Environment<'stx> {
    fn new_variable(&self) -> usize {
        self.stx_env.new_variable()
    }

    fn atomize(&mut self, e: syntax::Expression, then: impl FnOnce(Atom, &mut Self) -> Expression) -> Expression {
        use syntax::Expression as stx;
        match e {
            stx::CorePrimitive(s) => then(Atom::CorePrimitive(s), self),
            stx::Variable(i) => then(Atom::Variable(ValueID(i)), self),
            stx::Literal(syntax::Literal::Copy(x)) => then(Atom::Literal(x), self),
            _ => {
                let x = ValueID(self.new_variable());
                let k = then(Atom::Variable(x), self);
                self.new_expression(e, Continuation::Def(ContinuationDef {
                    formals: (vec![x], None),
                    body: Box::new(k),
                }))
            },
        }
    }
    fn atomize_many(&mut self, ee: Vec<syntax::Expression>, then: impl FnOnce(Vec<Atom>, &mut Self) -> Expression) -> Expression {
        use syntax::Expression as stx;
        let (atoms, new_ids) = ee.iter().map(|e| match e {
            stx::CorePrimitive(s) => (Atom::CorePrimitive(s), None),
            stx::Variable(i) => (Atom::Variable(ValueID(*i)), None),
            stx::Literal(syntax::Literal::Copy(x)) => (Atom::Literal(*x), None),
            _ => {
                let x = ValueID(self.new_variable());
                (Atom::Variable(x), Some(x))
            },
        }).collect::<(Vec<_>, Vec<_>)>();
        std::iter::zip(new_ids.into_iter(), ee.into_iter()).rev()
            .fold(then(atoms, self), |k, (id, e)| match id {
                None => k,
                Some(x) => self.new_expression(e, Continuation::Def(ContinuationDef {
                    formals: (vec![x], None),
                    body: Box::new(k),
                })),
            })
    }
    fn atomize_continuation(&mut self, k: Continuation, then: impl FnOnce(ContinuationRef, &mut Self) -> Expression) -> Expression {
        match k {
            Continuation::Ref(k) => then(k, self),
            Continuation::Def(k) => {
                let id = ContinuationID(self.new_variable());
                Expression::LetContinuation { id, k, body: Box::new(then(ContinuationRef::To(id), self)) }
            }
        }
    }
    fn new_expression(&mut self, e: syntax::Expression, k: Continuation) -> Expression {
        use syntax::Expression as stx;
        use Expression::*;

        match e {
            stx::CorePrimitive(s) => Continue {
                k,
                values: (vec![Atom::CorePrimitive(s)], None),
            },
            stx::Variable(i) => Continue {
                k,
                values: (vec![Atom::Variable(ValueID(i))], None),
            },
            stx::Literal(syntax::Literal::Copy(x)) => Continue {
                k,
                values: (vec![Atom::Literal(x)], None),
            },
            stx::Literal(syntax::Literal::NoCopy(val)) => {
                let id = ValueID(self.new_variable());
                LetLiteral {
                    id,
                    val,
                    body: Box::new(Continue {
                        k,
                        values: (vec![Atom::Variable(id)], None),
                    }),
                }
            },
            stx::ProcedureCall { operator, operands } => {
                self.atomize(*operator, move |operator, env|
                    env.atomize_many(operands, move |operands, _|
                        Apply { operator, operands: (operands, None), kontract: Arity::Unknown, k }))
            },
            stx::Lambda { formals, body } => {
                let id = ValueID(self.new_variable());
                let formals = (formals.0.iter().map(ValueID::from).collect(), formals.1.map(|e| ValueID::from(e.as_ref())));
                let escape = ContinuationID(self.new_variable());
                let body = self.new_block(body, Continuation::from(escape));
                LetLambda {
                    id,
                    val: Lambda {
                        formals,
                        escape: ContinuationRef::To(escape),
                        body: Box::new(body),
                    },
                    body: Box::new(Continue {
                        k,
                        values: (vec![Atom::Variable(id)], None),
                    }),
                }
            },
            stx::Conditional { test, consequent, alternate } => {
                self.atomize_continuation(k, move |k, env|
                    env.atomize(*test, move |test, env| Branch {
                        test,
                        consequent: Box::new(env.new_expression(*consequent, Continuation::Ref(k))),
                        alternate: match alternate {
                            Some(e) => Box::new(env.new_expression(*e, Continuation::Ref(k))),
                            None => Box::new(Continue { k: Continuation::Ref(k), values: (vec![Atom::Literal(LiteralC::Boolean(false))], None) }),
                        },
                    }))
            },
            stx::Assignment { ids, value } | stx::Definition { ids, value } => {
                let x = ValueID(self.new_variable());
                self.new_expression(*value, Continuation::Def(ContinuationDef {
                    formals: (vec![], Some(x)),
                    body: Box::new(Assign {
                        vars: (ids.0.iter().map(ValueID::from).collect(), ids.1.map(|e| ValueID::from(e.as_ref()))),
                        values: (vec![], Some(x)),
                        k,
                    }),
                }))
            },
            stx::Block { body } => self.new_block(body, k),
        }
    }
    fn new_block(&mut self, e: Vec<syntax::Expression>, k: Continuation) -> Expression {
        use Expression::*;
        let mut ee = e.into_iter().rev();
        if let Some(tail) = ee.next() {
            ee.fold(self.new_expression(tail, k), |acc, e|
                self.new_expression(e, Continuation::Def(ContinuationDef { formals: (vec![], Some(ValueID::invalid())), body: Box::new(acc) })))
        } else {
            Continue { k, values: (vec![], None) }
        }
    }

    fn optimize(&mut self, e: &mut Expression) -> Result<bool> {
        use Expression::*;
        match e {
            TakenOut => unreachable!(),
            LetLiteral { id, body, .. } if !self.visible_bindings.contains(id) => {
                let uses = body.uses_variable(*id);
                if uses == 0 {
                    *e = std::mem::take(body);
                    Ok(true)
                } else if uses == 1 {
                    Ok(body.visit_mut(false, |modified, e| ControlFlow::Continue(match e {
                        Assign { .. } => e.hoist_assignment(id),
                        _ => modified,
                    })).continue_value().unwrap())
                } else {
                    Ok(false)
                }
            },
            LetLambda { id, val, body } if !self.visible_bindings.contains(id) => {
                let uses = body.uses_variable(*id);
                let applies = body.visit(0, |count, e| match e {
                    Apply { operator: Atom::Variable(operator), .. } if operator == id => ControlFlow::Continue(count + 1),
                    _ => ControlFlow::Continue(count),
                }).continue_value().unwrap();
                let is_recursive = val.body.visit((), |_, e| match e {
                    Apply { operator: Atom::Variable(operator), .. } if operator == id => ControlFlow::Break(()),
                    _ => ControlFlow::Continue(()),
                }).is_break();
                if uses == 0 {
                    *e = std::mem::take(body);
                    Ok(true)
                } else if uses == 1 && applies == 0 {
                    Ok(body.visit_mut(false, |modified, e| ControlFlow::Continue(match e {
                        Assign { .. } => e.hoist_assignment(id),
                        _ => modified,
                    })).continue_value().unwrap())
                } else if uses == applies && !is_recursive && (applies == 1 || val.body.complexity() < BETA_EXPANSION_COMPLEXITY_THRESHOLD) {
                    body.visit_mut((), |_, e| match e {
                        Apply { operator: Atom::Variable(operator), operands, k, .. } if operator == id => {
                            let mut n = *if applies == 1 { std::mem::take(&mut val.body) } else { val.body.clone() };
                            n.rename(&val.formals, &*operands);
                            n.recontinue(val.escape, std::mem::take(k));
                            *e = n;
                            ControlFlow::Continue(())
                        },
                        _ => ControlFlow::Continue(()),
                    });
                    *e = std::mem::take(body);
                    Ok(true)
                } else {
                    Ok(false)
                }
            },
            LetContinuation { id, k, body } => {
                let (uses, applies) = body.visit((0, 0), |(uses, applies), e| match e {
                    Apply { k: Continuation::Ref(ContinuationRef::To(k)), .. } if k == id => ControlFlow::Continue((uses + 1, applies)),
                    Continue { k: Continuation::Ref(ContinuationRef::To(k)), .. } if k == id => ControlFlow::Continue((uses + 1, applies + 1)),
                    Assign { k: Continuation::Ref(ContinuationRef::To(k)), .. } if k == id => ControlFlow::Continue((uses + 1, applies)),
                    _ => ControlFlow::Continue((uses, applies)),
                }).continue_value().unwrap();

                if uses == 0 {
                    *e = std::mem::take(body);
                    Ok(true)
                } else if uses == applies && k.body.complexity() < BETA_EXPANSION_COMPLEXITY_THRESHOLD {
                    body.visit_mut((), |_, e| match e {
                        Continue { k: Continuation::Ref(ContinuationRef::To(k_id)), values } if k_id == id => {
                            let mut n = *if applies == 1 { std::mem::take(&mut k.body) } else { k.body.clone() };
                            n.rename(&k.formals, &*values);
                            *e = n;
                            ControlFlow::Continue(())
                        },
                        _ => ControlFlow::Continue(()),
                    });
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Apply { operator, operands: (operands, operands_tail), kontract, k } => {
                match operator {
                    Atom::CorePrimitive("apply") => {
                        *e = Apply {
                            operator: *operands.first().unwrap(),
                            operands: (operands.split_off(1), *operands_tail),
                            kontract: *kontract,
                            k: std::mem::take(k),
                        };
                        Ok(true)
                    },
                    Atom::CorePrimitive("call/cc") => {
                        let stored_dynamic_extent = ValueID(self.new_variable());
                        let reified_cont = ValueID(self.new_variable());
                        let values = ValueID(self.new_variable());
                        *e = self.atomize_continuation(std::mem::take(k), |k, _| Apply {
                            operator: Atom::CorePrimitive("__store_dynamic_extent"),
                            operands: (vec![], None),
                            kontract: Arity::Exact(1),
                            k: Continuation::Def(ContinuationDef {
                                formals: (vec![stored_dynamic_extent], None),
                                body: Box::new(LetLambda {
                                    id: reified_cont,
                                    val: Lambda {
                                        formals: (vec![], Some(values)),
                                        escape: ContinuationRef::Escape,
                                        body: Box::new(Apply {
                                            operator: Atom::CorePrimitive("__rewind_dynamic_extent"),
                                            operands: (vec![Atom::Variable(stored_dynamic_extent)], None),
                                            kontract: Arity::Exact(0),
                                            k: Continuation::Def(ContinuationDef {
                                                formals: (vec![], None),
                                                body: Box::new(Continue { k: Continuation::Ref(k), values: (vec![], Some(values)) }),
                                            }),
                                        }),
                                    },
                                    body: Box::new(Apply {
                                        operator: *operands.first().unwrap(),
                                        operands: (vec![Atom::Variable(reified_cont)], None),
                                        kontract: *kontract,
                                        k: Continuation::Ref(k),
                                    }),
                                }),
                            }),
                        });
                        Ok(true)
                    },
                    Atom::CorePrimitive("call-with-values") => {
                        let values = ValueID(self.new_variable());
                        *e = Apply {
                            operator: *operands.get(0).unwrap(),
                            operands: (vec![], None),
                            kontract: Arity::Unknown,
                            k: Continuation::Def(ContinuationDef {
                                formals: (vec![], Some(values)),
                                body: Box::new(Apply {
                                    operator: *operands.get(1).unwrap(),
                                    operands: (vec![], Some(values)),
                                    kontract: Arity::AtLeast(0),
                                    k: std::mem::take(k),
                                }),
                            }),
                        };
                        Ok(true)
                    },
                    Atom::CorePrimitive("dynamic-wind") => {
                        let stored_dynamic_extent = ValueID(self.new_variable());
                        let thunk_values = ValueID(self.new_variable());
                        *e = Apply {
                            operator: *operands.get(0).unwrap(),
                            operands: (vec![], None),
                            kontract: Arity::Exact(0),
                            k: Continuation::Def(ContinuationDef {
                                formals: (vec![], None),
                                body: Box::new(Apply {
                                    operator: Atom::CorePrimitive("__push_dynamic_frame"),
                                    operands: (vec![*operands.get(0).unwrap(), *operands.get(2).unwrap()], None),
                                    kontract: Arity::Exact(1),
                                    k: Continuation::Def(ContinuationDef {
                                        formals: (vec![stored_dynamic_extent], None),
                                        body: Box::new(Apply {
                                            operator: *operands.get(1).unwrap(),
                                            operands: (vec![], None),
                                            kontract: Arity::AtLeast(0),
                                            k: Continuation::Def(ContinuationDef {
                                                formals: (vec![], Some(thunk_values)),
                                                body: Box::new(Apply {
                                                    operator: Atom::CorePrimitive("__rewind_dynamic_extent"),
                                                    operands: (vec![Atom::Variable(stored_dynamic_extent)], None),
                                                    kontract: Arity::Exact(0),
                                                    k: Continuation::Def(ContinuationDef {
                                                        formals: (vec![], None),
                                                        body: Box::new(Continue {
                                                            k: std::mem::take(k),
                                                            values: (vec![], Some(thunk_values)),
                                                        }),
                                                    }),
                                                }),
                                            }),
                                        }),
                                    }),
                                }),
                            }),
                        };
                        Ok(true)
                    },
                    Atom::CorePrimitive("values") => {
                        *e = Continue {
                            k: std::mem::take(k),
                            values: (std::mem::take(operands), *operands_tail),
                        };
                        Ok(true)
                    },
                    Atom::CorePrimitive("raise") => {
                        let eh = ValueID(self.new_variable());
                        let old_extent = ValueID(self.new_variable());
                        *e = Apply {
                            operator: Atom::CorePrimitive("__find_exception_handler"),
                            operands: (vec![], None),
                            kontract: Arity::Exact(2),
                            k: Continuation::Def(ContinuationDef {
                                formals: (vec![eh, old_extent], None),
                                body: Box::new(Apply {
                                    operator: Atom::CorePrimitive("__rewind_dynamic_extent"),
                                    operands: (vec![Atom::Variable(old_extent)], None),
                                    kontract: Arity::Exact(0),
                                    k: Continuation::Def(ContinuationDef {
                                        formals: (vec![], None),
                                        body: Box::new(Apply {
                                            operator: Atom::Variable(eh),
                                            operands: (std::mem::take(operands), *operands_tail),
                                            kontract: Arity::Unknown,
                                            k: Continuation::Ref(ContinuationRef::Abort),
                                        }),
                                    }),
                                }),
                            }),
                        };
                        Ok(true)
                    },
                    Atom::CorePrimitive("raise-continuable") => {
                        let eh = ValueID(self.new_variable());
                        *e = Apply {
                            operator: Atom::CorePrimitive("__find_exception_handler"),
                            operands: (vec![], None),
                            kontract: Arity::Exact(2),
                            k: Continuation::Def(ContinuationDef {
                                formals: (vec![eh, ValueID::invalid()], None),
                                body: Box::new(Apply {
                                    operator: Atom::Variable(eh),
                                    operands: (std::mem::take(operands), *operands_tail),
                                    kontract: Arity::AtLeast(0),
                                    k: std::mem::take(k),
                                }),
                            }),
                        };
                        Ok(true)
                    },
                    Atom::CorePrimitive("with-exception-handler") => {
                        let dynamic_extent = ValueID(self.new_variable());
                        let thunk_values = ValueID(self.new_variable());
                        *e = Apply {
                            operator: Atom::CorePrimitive("__push_exception_handler"),
                            operands: (vec![*operands.get(0).unwrap()], None),
                            kontract: Arity::Exact(1),
                            k: Continuation::Def(ContinuationDef {
                                formals: (vec![dynamic_extent], None),
                                body: Box::new(Apply {
                                    operator: *operands.get(1).unwrap(),
                                    operands: (vec![], None),
                                    kontract: Arity::Unknown,
                                    k: Continuation::Def(ContinuationDef {
                                        formals: (vec![], Some(thunk_values)),
                                        body: Box::new(Apply {
                                            operator: Atom::CorePrimitive("__rewind_dynamic_extent"),
                                            operands: (vec![Atom::Variable(dynamic_extent)], None),
                                            kontract: Arity::Exact(0),
                                            k: Continuation::Def(ContinuationDef {
                                                formals: (vec![], None),
                                                body: Box::new(Continue {
                                                    k: std::mem::take(k),
                                                    values: (vec![], Some(thunk_values)),
                                                }),
                                            }),
                                        }),
                                    }),
                                }),
                            }),
                        };
                        Ok(true)
                    },
                    _ => Ok(false),
                }
            }
            Branch { test, consequent, alternate } => match test {
                Atom::Literal(LiteralC::Boolean(false)) => {
                    *e = std::mem::take(alternate);
                    Ok(true)
                },
                Atom::Literal(_) | Atom::CorePrimitive(_) => {
                    *e = std::mem::take(consequent);
                    Ok(true)
                },
                _ => Ok(false),
            },
            Continue { k: Continuation::Def(ContinuationDef { formals, body }), values } => {
                body.rename(formals, &*values);
                *e = std::mem::take(body);
                Ok(true)
            },
            Assign { vars, k, .. } if vars.0.is_empty() && vars.1.is_none() => {
                *e = Continue { k: std::mem::take(k), values: (vec![], None) };
                Ok(true)
            },
            _ => Ok(false),
        }
    }
}

pub fn transform(e: syntax::Expression, k: ContinuationRef, env: &syntax::Environment) -> Result<Expression> {
    let mut env = Environment::from(env);
    let mut e = env.new_expression(e, Continuation::Ref(k));
    e.check_arity()?;

    loop {
        let next = e.visit_mut(Ok(&mut env), |acc, e| {
            let env = acc.unwrap();
            match env.optimize(e) {
                Ok(true) => ControlFlow::Break(Ok(env)),
                Ok(false) => ControlFlow::Continue(Ok(env)),
                Err(e) => ControlFlow::Break(Err(e)),
            }
        });
        if next.is_break() {
            next.break_value().unwrap()?;
            e.check_arity()?;
        } else {
            e.check_arity()?;
            return Ok(e);
        }
    }
}
