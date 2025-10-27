use crate::io::InputPort;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use super::frontend;
use super::frontend::CoreForm;
use super::{Binding, Literal, LiteralC, LiteralD, Located, Location};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Scope(usize);
type ScopeSet = BTreeSet<Scope>;

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(Literal),
    Variable(usize),
    Core(&'static str),
    Call { operator: Box<Located<Expression>>, operands: Vec<Located<Expression>> },
    Lambda { formals: (Vec<Located<usize>>, Option<Located<usize>>), body: Vec<Located<Expression>> },
    If { test: Box<Located<Expression>>, consequent: Box<Located<Expression>>, alternate: Option<Box<Located<Expression>>> },
    Assign { ids: (Vec<Located<usize>>, Option<Located<usize>>), values: Box<Located<Expression>> },
    Block { body: Vec<Located<Expression>> },
}
impl Expression {
    fn at(self, location: Location) -> Located<Self> {
        Located { item: self, location }
    }
    fn introduced(self) -> Located<Self> {
        Located { item: self, location: Default::default() }
    }
}
impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Expression::*;
        fn format_formals(formals: &(Vec<Located<usize>>, Option<Located<usize>>)) -> String {
            let formals0 = formals.0.iter()
                .map(|i| format!("x{}", **i))
                .collect::<Vec<_>>().join(" ");
            if let Some(Located { item, .. }) = formals.1 {
                if formals0.len() > 0 {
                    format!("({} . x{})", formals0, item)
                } else {
                    format!("x{}", item)
                }
            } else {
                format!("({})", formals0)
            }
        }

        match self {
            Literal(literal) => match literal {
                super::Literal::Copy(LiteralC::Nil) => write!(f, "'{}", literal),
                super::Literal::NoCopy(LiteralD::Nil | LiteralD::Symbol(_) | LiteralD::List(..)) => write!(f, "'{}", literal),
                _ => write!(f, "{}", literal),
            },
            Variable(i) => write!(f, "x{}", i),
            Core(s) => s.fmt(f),
            Call { operator, operands } => {
                write!(f, "({}", &***operator)?;
                for operand in operands {
                    write!(f, " {}", &**operand)?;
                }
                write!(f, ")")
            },
            Lambda { formals, body } => {
                write!(f, "(\u{03BB} {}", format_formals(formals))?;
                for body in body {
                    write!(f, " {}", &**body)?;
                }
                write!(f, ")")
            },
            If { test, consequent, alternate } => {
                write!(f, "(if {} {}", &***test, &***consequent)?;
                if let Some(alternate) = alternate {
                    write!(f, " {}", &***alternate)?;
                }
                write!(f, ")")
            },
            Assign { ids, values } => write!(f, "(set-values! {} {})", format_formals(ids), &***values),
            Block { body } => {
                write!(f, "(begin")?;
                for body in body {
                    write!(f, " {}", &**body)?;
                }
                write!(f, ")")
            },
        }
    }
}


#[derive(Debug, Clone)]
pub struct Object {
    e: ObjectT,
    children: Vec<Object>,
    scopes: ScopeSet,
    location: Location,
}
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectT {
    Boolean(bool),
    Integer(i64),
    Float(f32),
    Character(char),
    String(String),
    Symbol(Cow<'static, str>),
    Bytes(Vec<u8>),
    List,
    ImproperList,
    Vector,
    Quote,
    Quasiquote,
    Unquote,
    UnquoteSplicing,
}
impl Object {
    fn new(e: ObjectT) -> Self {
        Self {
            e,
            children: Vec::new(),
            scopes: Default::default(),
            location: Default::default(),
        }
    }
    fn with_children(mut self, children: Vec<Object>) -> Self {
        self.children = children;
        self
    }
    fn with_scopes(mut self, scopes: ScopeSet) -> Self {
        self.scopes = scopes;
        self
    }
    fn with_source(mut self, location: Location) -> Self {
        self.location = location;
        self
    }

    fn add_scope(&mut self, scope: Scope) {
        use ObjectT::*;
        let scopeable = |stx: &Object| match stx.e {
            Symbol(_) | Quote | List | ImproperList | Quasiquote | Unquote | UnquoteSplicing => true,
            _ => false,
        };

        if scopeable(self) {
            self.scopes.insert(scope);
            self.children.iter_mut()
                .filter(|stx| scopeable(stx))
                .for_each(|stx| stx.add_scope(scope));
        }
    }
}
impl PartialEq<Literal> for Object {
    fn eq(&self, other: &Literal) -> bool {
        use Literal::*;
        use LiteralC::*;
        use LiteralD::*;
        if self.children.is_empty() {
            match (&self.e, other) {
                (ObjectT::Boolean(p), Copy(Boolean(q))) => p == q,
                (ObjectT::Integer(p), Copy(Integer(q))) => p == q,
                (ObjectT::Float(p), Copy(Float(q))) => p == q,
                (ObjectT::Character(p), Copy(Character(q))) => p == q,
                (ObjectT::String(p), NoCopy(String(q))) => p == q,
                (ObjectT::Symbol(p), NoCopy(Static(q))) => p == q,
                (ObjectT::Symbol(p), NoCopy(Symbol(q))) => p == q,
                (ObjectT::Bytes(p), NoCopy(Bytes(q))) => p == q,
                _ => false,
            }
        } else {
            &Literal::from(self) == other
        }
    }
}
impl std::fmt::Display for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_inner(this: &Object, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            use ObjectT::*;
            match &this.e {
                Boolean(true) => write!(f, "#t"),
                Boolean(false) => write!(f, "#f"),
                Integer(x) => write!(f, "{}", x),
                Float(x) => write!(f, "{}", x),
                Character(x) => write!(f, "#\\{}", x),
                String(x) => write!(f, "{:?}", x),
                Symbol(x) => write!(f, "{}", x),
                Bytes(x) => {
                    write!(f, "#u8(")?;
                    for (i, x) in x.iter().enumerate() {
                        let at_end = (this.children.len() - 1) == i;
                        write!(f, "{}{}", x, if at_end { "" } else { " "})?;
                    }
                    write!(f, ")")
                }
                Quote => {
                    write!(f, "(quote ")?;
                    fmt_inner(this.children.first().unwrap(), f)?;
                    write!(f, ")")
                },
                Quasiquote => {
                    write!(f, "(quasiquote ")?;
                    fmt_inner(this.children.first().unwrap(), f)?;
                    write!(f, ")")
                },
                Unquote => {
                    write!(f, "(unquote ")?;
                    fmt_inner(this.children.first().unwrap(), f)?;
                    write!(f, ")")
                },
                UnquoteSplicing => {
                    write!(f, "(unquote-splicing ")?;
                    fmt_inner(this.children.first().unwrap(), f)?;
                    write!(f, ")")
                },
                List | ImproperList | Vector => {
                    write!(f, "{}(", if this.e == Vector { "#" } else { "" })?;
                    for (i, child) in this.children.iter().enumerate() {
                        let at_end = (this.children.len() - 1) == i;
                        if this.e == ImproperList && at_end { write!(f, ". ")?; }
                        fmt_inner(child, f)?;
                        write!(f, "{}", if at_end { "" } else { " "})?;
                    }
                    write!(f, ")")
                },
            }
        }
        fmt_inner(self, f)?;
        if self.location.is_some() {
            write!(f, ":{}", self.location)?;
        }
        Ok(())
    }
}
impl From<&Object> for Literal {
    fn from(stx: &Object) -> Self {
        use super::LiteralC::*;
        use super::LiteralD::*;
        match &stx.e {
            ObjectT::Boolean(x) => Boolean(*x).into(),
            ObjectT::Integer(x) => Integer(*x).into(),
            ObjectT::Float(x) => Float(*x).into(),
            ObjectT::Character(x) => Character(*x).into(),
            ObjectT::String(x) => String(x.clone()).into(),
            ObjectT::Symbol(Cow::Borrowed(x)) => Static(x).into(),
            ObjectT::Symbol(Cow::Owned(x)) => Symbol(x.clone()).into(),
            ObjectT::Bytes(x) => Bytes(x.clone()).into(),
            ObjectT::List | ObjectT::ImproperList if stx.children.len() == 0 => super::LiteralC::Nil.into(),
            ObjectT::List => {
                let operator = if let ObjectT::Symbol(name) = &stx.children.first().unwrap().e {
                    match name.as_ref() {
                        "quote" => Some("quote"),
                        "quasiquote" => Some("quasiquote"),
                        "unquote" => Some("unquote"),
                        "unquote-splicing" => Some("unquote-splicing"),
                        _ => None,
                    }
                } else {
                    None
                };
                match operator {
                    Some(operator) =>
                        List(std::iter::once(Literal::NoCopy(Static(operator)))
                            .chain(stx.children.iter().skip(1).map(Literal::from))
                            .collect(), None).into(),
                    None => List(stx.children.iter().map(Literal::from).collect(), None).into(),
                }
            },
            ObjectT::ImproperList => {
                let mut children = stx.children.iter().map(Literal::from).collect::<Vec<Literal>>();
                let tail = children.pop().map(Box::new);
                List(children, tail).into()
            },
            ObjectT::Vector => Vector(stx.children.iter().map(Literal::from).collect()).into(),
            ObjectT::Quote => List(std::iter::once(Literal::NoCopy(Static("quote"))).chain(stx.children.iter().map(Literal::from)).collect(), None).into(),
            ObjectT::Quasiquote => List(std::iter::once(Literal::NoCopy(Static("quasiquote"))).chain(stx.children.iter().map(Literal::from)).collect(), None).into(),
            ObjectT::Unquote => List(std::iter::once(Literal::NoCopy(Static("unquote"))).chain(stx.children.iter().map(Literal::from)).collect(), None).into(),
            ObjectT::UnquoteSplicing => List(std::iter::once(Literal::NoCopy(Static("unquote-splicing"))).chain(stx.children.iter().map(Literal::from)).collect(), None).into(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Pattern {
    Underscore,
    Constant(Literal),
    Literal(Cow<'static, str>),
    Variable(Cow<'static, str>),
    Ellipsis(Box<Pattern>),
    List(Vec<Pattern>),
    ImproperList(Vec<Pattern>, Box<Pattern>),
    Vector(Vec<Pattern>),
}
#[derive(Debug, Clone)]
pub enum Template {
    Constant(Object),
    Identifier(Cow<'static, str>),
    Ellipsis(Box<Template>),
    List(Vec<Template>),
    ImproperList(Vec<Template>, Box<Template>),
    Vector(Vec<Template>),
}
#[derive(Debug, Clone)]
pub struct Rules(pub Vec<(Pattern, Template)>);
impl std::ops::Deref for Rules {
    type Target = Vec<(Pattern, Template)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub enum MatchValue<'a> {
    One(&'a Object),
    Many(Vec<MatchValue<'a>>),
}
#[derive(Debug, Clone, Default)]
pub struct MatchEnv<'a, 'c>(HashMap<&'a str, MatchValue<'c>>);
#[derive(Debug, Clone, Default)]
struct MatchSlice<'a, 'b, 'c>(HashMap<&'a str, &'b MatchValue<'c>>);
impl<'a, 'c> MatchEnv<'a, 'c> {
    fn new() -> Self {
        Self(HashMap::new())
    }
    fn single(key: &'a str, value: MatchValue<'c>) -> Self {
        let mut this = HashMap::new();
        this.insert(key, value);
        Self(this)
    }
    fn extended(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        self
    }
    fn with(self, key: &'a str, value: MatchValue<'c>) -> Self {
        self.extended(Self::single(key, value))
    }
}
impl<'a, 'c> std::ops::Deref for MatchEnv<'a, 'c> {
    type Target = HashMap<&'a str, MatchValue<'c>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'a, 'c> std::ops::DerefMut for MatchEnv<'a, 'c> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl<'a, 'b, 'c> std::ops::Deref for MatchSlice<'a, 'b, 'c> {
    type Target = HashMap<&'a str, &'b MatchValue<'c>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'a, 'b, 'c> From<&'b MatchEnv<'a, 'c>> for MatchSlice<'a, 'b, 'c> {
    fn from(env: &'b MatchEnv<'a, 'c>) -> Self {
        Self(env.0.iter().map(|(k, v)| (*k, v)).collect())
    }
}
impl<'a, 'b, 'c> MatchSlice<'a, 'b, 'c> {
    fn slice(&self, i: usize) -> Self {
        Self(self.0.iter().map(|(k, v)| (*k, match v {
            MatchValue::One(_) => v,
            MatchValue::Many(vs) => &vs[i],
        })).collect())
    }
}

impl<'a, 'b> Pattern {
    fn new(stx: &Object, literals: &HashSet<&str>) -> Result<Self, String> {
        use ObjectT::*;
        match &stx.e {
            Boolean(_) | Integer(_) | Float(_) | Character(_) | String(_) | Bytes(_) => Ok(Pattern::Constant(Literal::from(stx))),
            Symbol(s) if s == "_" => Ok(Pattern::Underscore),
            Symbol(s) if literals.contains(s.as_ref()) => Ok(Pattern::Literal(s.clone())),
            Symbol(s) => Ok(Pattern::Variable(s.clone())),
            List => Ok(Pattern::List(Self::new_sequence(stx.children.iter(), literals)?)),
            ImproperList => {
                let mut children = stx.children.iter();
                let tail = children.next_back().unwrap();
                Ok(Pattern::ImproperList(Self::new_sequence(children, literals)?, Box::new(Self::new(tail, literals)?)))
            },
            Vector => Ok(Pattern::Vector(Self::new_sequence(stx.children.iter(), literals)?)),
            Quote | Quasiquote | Unquote | UnquoteSplicing => Err(format!("syntax-rules: unexpected: {}", stx)),
        }
    }
    fn new_sequence(stxes: impl Iterator<Item = &'a Object>, literals: &HashSet<&str>) -> Result<Vec<Self>, String> {
        let mut stxes = stxes.peekable();
        let mut patterns = Vec::new();
        while let Some(stx) = stxes.next() {
            let subpat = Pattern::new(stx, literals)?;
            let ellipsis = stxes.next_if(|x| match x.e {
                ObjectT::Symbol(ref lookahead) if lookahead == "..." => true,
                _ => false,
            }).is_some();
            if ellipsis {
                patterns.push(Pattern::Ellipsis(Box::new(subpat)));
            } else {
                patterns.push(subpat);
            }
        }
        Ok(patterns)
    }

    pub fn try_match(&'a self, stx: &'b Object) -> Option<MatchEnv<'a, 'b>> {
        match (self, &stx.e) {
            (Pattern::Underscore, _) => Some(Default::default()),
            (Pattern::Constant(pattern), _) if Literal::from(stx) == *pattern => Some(Default::default()),
            (Pattern::Literal(pattern), ObjectT::Symbol(input)) if pattern == input => Some(Default::default()),
            (Pattern::Variable(pattern), _) => Some(MatchEnv::single(pattern.as_ref(), MatchValue::One(stx))),
            (Pattern::List(patterns), ObjectT::List) => Self::try_match_sequence(patterns, stx.children.iter(), None),
            (Pattern::ImproperList(patterns, tail), ObjectT::List) => Self::try_match_sequence(patterns, stx.children.iter(), Some(tail)),
            (Pattern::ImproperList(patterns, tail), ObjectT::ImproperList) => {
                let mut inputs = stx.children.iter();
                let tail_input = inputs.next_back().unwrap();
                match (Self::try_match_sequence(patterns, inputs, None), tail.try_match(tail_input)) {
                    (Some(env), Some(tail_env)) => Some(env.extended(tail_env)),
                    _ => None,
                }
            },
            (Pattern::Vector(patterns), ObjectT::Vector) => Self::try_match_sequence(patterns, stx.children.iter(), None),
            _ => None,
        }
    }
    fn try_match_sequence(patterns: &'a [Pattern], mut inputs: impl ExactSizeIterator<Item = &'b Object>, tail: Option<&'a Pattern>) -> Option<MatchEnv<'a, 'b>> {
        let mut patterns = patterns.iter();
        let mut env = MatchEnv::new();

        while let Some(pattern) = patterns.next() {
            if let Pattern::Ellipsis(pattern) = pattern {
                while let Some(Some(e)) = inputs.next().map(|stx| pattern.try_match(stx)) {
                    for (k, v) in e.0 {
                        match env.entry(k).or_insert_with(|| MatchValue::Many(vec![])) {
                            MatchValue::Many(vs) => vs.push(v),
                            MatchValue::One(_) => panic!(),
                        }
                    }
                }
            } else if let Some(Some(e)) = inputs.next().map(|stx| pattern.try_match(stx)) {
                env = env.extended(e);
            } else {
                return None;
            }
        }

        match tail {
            None if inputs.len() == 0 => Some(env),
            None => None,
            Some(Pattern::Underscore) => Some(env),
            Some(Pattern::Constant(_) | Pattern::Literal(_)) => None,
            Some(Pattern::Variable(pattern)) => Some(env.with(pattern.as_ref(), MatchValue::Many(inputs.map(MatchValue::One).collect()))),
            Some(Pattern::Ellipsis(_)) => None,
            Some(Pattern::List(patterns) | Pattern::Vector(patterns)) => Some(env.extended(Self::try_match_sequence(patterns, inputs, None)?)),
            Some(Pattern::ImproperList(patterns, tail)) => Some(env.extended(Self::try_match_sequence(patterns, inputs, Some(tail))?)),
        }
    }
}
impl std::fmt::Display for Pattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pattern::Underscore => write!(f, "_"),
            Pattern::Constant(pattern) => write!(f, "{}", pattern),
            Pattern::Literal(pattern) => write!(f, "{}", pattern),
            Pattern::Variable(pattern) => write!(f, "{}", pattern),
            Pattern::Ellipsis(pattern) => write!(f, "{} ...", pattern),
            Pattern::List(patterns) | Pattern::Vector(patterns) => {
                write!(f, "{}(", if matches!(self, Pattern::Vector(_)) { "#" } else { "" })?;
                for (i, child) in patterns.iter().enumerate() {
                    match patterns.len() - i {
                        1 => write!(f, "{})", child),
                        _ => write!(f, "{} ", child),
                    }?;
                }
                Ok(())
            },
            Pattern::ImproperList(patterns, tail) => {
                write!(f, "(")?;
                patterns.iter()
                    .map(|child| write!(f, "{} ", child))
                    .collect::<Result<(), _>>()?;
                write!(f, ". {})", tail)
            },
        }
    }
}

impl<'a> Template {
    fn from(stx: &Object) -> Result<Self, String> {
        Self::new(stx, false)
    }
    fn new(stx: &Object, ellipsis_literal: bool) -> Result<Self, String> {
        use ObjectT::*;
        if let Symbol(ref s) = stx.e {
            Ok(Template::Identifier(s.clone()))
        } else if List == stx.e {
            let new_literal_ellipsis = stx.children.first().map(|stx| match stx.e {
                Symbol(ref s) if s == "..." => true,
                _ => false,
            }).unwrap_or(false);
            if new_literal_ellipsis {
                if stx.children.len() == 2 {
                    Self::new(&stx.children[1], true)
                } else {
                    Err(format!("syntax-rules: invalid template: {}", stx))
                }
            } else {
                Ok(Template::List(Self::new_sequence(stx.children.iter(), ellipsis_literal)?))
            }
        } else if ImproperList == stx.e {
            let mut children = stx.children.iter();
            let tail = children.next_back().unwrap();
            Ok(Template::ImproperList(Self::new_sequence(children, ellipsis_literal)?, Box::new(Self::new(tail, ellipsis_literal)?)))
        } else if Vector == stx.e {
            Ok(Template::Vector(Self::new_sequence(stx.children.iter(), ellipsis_literal)?))
        } else {
            Ok(Template::Constant(stx.clone()))
        }
    }
    fn new_sequence(stxes: impl Iterator<Item = &'a Object>, ellipsis_literal: bool) -> Result<Vec<Self>, String> {
        let mut stxes = stxes.peekable();
        let mut templates = Vec::new();
        while let Some(stx) = stxes.next() {
            let ellipsis_following = !ellipsis_literal && stxes.next_if(|stx| match stx.e {
                ObjectT::Symbol(ref s) if s == "..." => true,
                _ => false
            }).is_some();
            if ellipsis_following {
                templates.push(Template::Ellipsis(Box::new(Template::new(stx, false)?)));
            } else {
                templates.push(Template::new(stx, false)?);
            }
        }
        Ok(templates)
    }

    fn count_ellipsis_reps(&self, env: &MatchSlice) -> Result<usize, String> {
        use Template::*;
        let mut queue = std::iter::once(self).collect::<std::collections::VecDeque<_>>();
        let mut reps = None;
        while let Some(template) = queue.pop_front() {
            match template {
                Constant(_) => (),
                Identifier(template) => match env.get(template.as_ref()) {
                    Some(MatchValue::Many(vs)) => {
                        if reps.is_some_and(|reps| reps != vs.len()) {
                            return Err(format!("syntax-rules: mismatched ellipsis in template"));
                        } else if reps.is_none() {
                            reps.replace(vs.len());
                        }
                    },
                    Some(MatchValue::One(_)) => panic!(),
                    None => (),
                },
                Ellipsis(template) => queue.push_back(template.as_ref()),
                List(templates) | Vector(templates) => queue.extend(templates),
                ImproperList(templates, tail) => queue.extend(templates.iter().chain(std::iter::once(tail.as_ref()))),
            }
        }
        Ok(reps.unwrap_or(0))
    }
    fn apply(&self, env: &MatchSlice, stx: &Object, expand_scope: Scope) -> Result<Object, String> {
        use Template::*;
        match self {
            Constant(stx) => Ok(stx.clone()),
            Identifier(template) => match env.get(template.as_ref()) {
                Some(MatchValue::One(stx)) => Ok(Object::clone(stx)),
                Some(MatchValue::Many(_)) => Err(format!("syntax-rules: bad macro template: {} should have an ellipsis", template.as_ref())),
                None => Ok(Object {
                    e: ObjectT::Symbol(template.clone()),
                    children: vec![],
                    scopes: stx.scopes.iter().copied().chain(std::iter::once(expand_scope)).collect(),
                    location: stx.location,
                }),
            },
            Ellipsis(_) => panic!(),
            List(templates) => Ok(Object {
                e: ObjectT::List,
                children: Self::apply_sequence(templates, env, stx, expand_scope)?,
                scopes: Default::default(),
                location: stx.location,
            }),
            ImproperList(templates, tail) => Ok(Object {
                e: ObjectT::List,
                children: Self::apply_sequence(templates, env, stx, expand_scope)?.into_iter()
                    .chain(std::iter::once(tail.apply(env, stx, expand_scope)?))
                    .collect(),
                scopes: Default::default(),
                location: stx.location,
            }),
            Vector(templates) => Ok(Object {
                e: ObjectT::Vector,
                children: Self::apply_sequence(templates, env, stx, expand_scope)?,
                scopes: Default::default(),
                location: stx.location,
            }),
        }
    }
    fn apply_sequence(templates: &[Template], env: &MatchSlice, stx: &Object, expand_scope: Scope) -> Result<Vec<Object>, String> {
        templates.iter().try_fold(Vec::with_capacity(templates.len()), |mut outputs, template| {
            if let Template::Ellipsis(template) = template {
                (0..template.count_ellipsis_reps(env)?).map(|i| env.slice(i))
                    .map(|env_slice| template.apply(&env_slice, stx, expand_scope))
                    .try_for_each(|app| app.map(|stx| outputs.push(stx)))?;
            } else {
                outputs.push(template.apply(env, stx, expand_scope)?);
            }
            Ok(outputs)
        })
    }
}
impl std::fmt::Display for Template {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Template::Constant(stx) => write!(f, "{}", stx),
            Template::Identifier(pattern) => write!(f, "{}", pattern),
            Template::Ellipsis(template) => write!(f, "{} ...", template),
            Template::List(templates) | Template::Vector(templates) => {
                write!(f, "{}(", if matches!(self, Template::Vector(_)) { "#" } else { "" })?;
                for (i, child) in templates.iter().enumerate() {
                    match templates.len() - i {
                        1 => write!(f, "{})", child),
                        _ => write!(f, "{} ", child),
                    }?;
                }
                Ok(())
            },
            Template::ImproperList(templates, tail) => {
                write!(f, "(")?;
                templates.iter()
                    .map(|child| write!(f, "{} ", child))
                    .collect::<Result<(), _>>()?;
                write!(f, ". {})", tail)
            },
        }
    }
}

impl Rules {
    fn from<P: IntoIterator<Item = Pattern>, T: IntoIterator<Item = Template>>(patterns: P, templates: T) -> Self {
        Self(std::iter::zip(patterns, templates).collect())
    }
}
impl std::fmt::Display for Rules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut literals = std::collections::HashSet::new();
        for (pattern, _) in self.0.iter() {
            let mut queue = std::iter::once(pattern).collect::<std::collections::VecDeque<_>>();
            while let Some(pattern) = queue.pop_front() {
                match pattern {
                    Pattern::Literal(Cow::Borrowed(s)) => { literals.insert(*s); },
                    Pattern::Literal(Cow::Owned(s)) => { literals.insert(s.as_str()); },
                    Pattern::Ellipsis(pattern) => queue.push_back(pattern),
                    Pattern::List(patterns) => queue.extend(patterns),
                    Pattern::ImproperList(patterns, tail) => queue.extend(patterns.iter().chain(std::iter::once(tail.as_ref()))),
                    Pattern::Vector(patterns) => queue.extend(patterns),
                    _ => (),
                }
            }
        }

        write!(f, "(syntax-rules (")?;
        for (i, t) in literals.iter().enumerate() {
            write!(f, "{}{}", t, if i + 1 == literals.len() { ')' } else { ' ' })?;
        }
        write!(f, "\n")?;
        for (i, (pattern, template)) in self.0.iter().enumerate() {
            write!(f, " ({}\n  {}){}", pattern, template, if i + 1 == self.0.len() { ")" } else { "\n" })?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Environment<'p> {
    parent_env: &'p mut frontend::Environment,
    bindings: HashMap<Cow<'static, str>, BTreeMap<ScopeSet, Binding>>,
    scope_counter: std::cell::Cell<usize>,
}
impl<'p> From<&'p mut frontend::Environment> for Environment<'p> {
    fn from(parent_env: &'p mut frontend::Environment) -> Self {
        let bindings = parent_env.toplevels.iter()
            .map(|(name, binding)| (name.clone(), std::iter::once((ScopeSet::default(), *binding)).collect()))
            .collect();
        Self {
            parent_env,
            bindings,
            scope_counter: std::cell::Cell::new(1),
        }
    }
}
impl std::ops::Deref for Environment<'_> {
    type Target = frontend::Environment;

    fn deref(&self) -> &Self::Target {
        self.parent_env
    }
}
impl std::ops::DerefMut for Environment<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.parent_env
    }
}
impl<'p> Environment<'p> {
    fn new_id(&self) -> usize {
        self.parent_env.new_id()
    }
    fn new_scope(&self) -> Scope {
        let scope_id = self.scope_counter.get();
        self.scope_counter.set(scope_id.wrapping_add(1));
        Scope(scope_id)
    }
    fn resolve(&self, stx: &Object) -> Option<Binding> {
        if let ObjectT::Symbol(ref name) = stx.e {
            if let Some(bindings) = self.bindings.get(name.as_ref()) {
                let mut candidates = bindings.iter().filter(|(scopes, _)| scopes.is_subset(&stx.scopes));
                if let Some((best_scopes, best_binding)) = candidates.clone().max_by_key(|(scopes, _)| scopes.len()) {
                    if candidates.all(|(scopes, _)| scopes.is_subset(best_scopes)) {
                        return Some(*best_binding);
                    } else {
                        panic!("ambiguous binding: {}", name);
                    }
                }
            }
        }
        None
    }

    fn add_binding_scopes(&mut self, mut stx: Object) -> Result<Object, String> {
        match stx.children.first().map(|head| self.resolve(head)) {
            Some(Some(Binding::CoreForm(CoreForm::Lambda))) => {
                let sc = self.new_scope();

                // Add the binding scope to every identifier in this binding construct
                stx.children.iter_mut().skip(1).for_each(|stx| stx.add_scope(sc));

                // Record the binding for each bound variable
                match stx.children.get(1).ok_or_else(|| format!("lambda: bad syntax: {}", stx))? {
                    Object { e: ObjectT::Symbol(name), scopes, .. } => {
                        // (lambda args ...)
                        let b = Binding::Variable(self.new_id());
                        if let Some(bindings) = self.bindings.get_mut(name.as_ref()) {
                            bindings
                        } else {
                            self.bindings.entry(name.clone()).or_default()
                        }.insert(scopes.clone(), b);
                    },
                    Object { e: ObjectT::List | ObjectT::ImproperList, children: parameters, .. } => {
                        // (lambda (param1 param2 ...) ...)
                        for param in parameters {
                            let b = Binding::Variable(self.new_id());
                            if let ObjectT::Symbol(ref name) = param.e {
                                if let Some(bindings) = self.bindings.get_mut(name.as_ref()) {
                                    bindings
                                } else {
                                    self.bindings.entry(name.clone()).or_default()
                                }.insert(param.scopes.clone(), b);
                            } else {
                                return Err(format!("lambda: not an identifier: {}", param));
                            }
                        }
                    },
                    _ => return Err(format!("lambda: bad argument sequence: {}", stx.children.get(1).unwrap())),
                };
            },
            Some(Some(Binding::CoreForm(CoreForm::Begin))) => {
                // Add a binding scope for any definitions here.
                let sc = self.new_scope();
                stx.children.iter_mut().skip(1).for_each(|stx| stx.add_scope(sc));
            },
            Some(Some(Binding::CoreForm(CoreForm::Quote))) => return Ok(stx), // if quoted, there are no bindings in here; skip it
            _ => (),
        }

        stx.children = stx.children.into_iter()
            .map(|stx| match stx.e {
                ObjectT::List | ObjectT::ImproperList => self.add_binding_scopes(stx),
                _ => Ok(stx),
            }).collect::<Result<Vec<_>, _>>()?;
        Ok(stx)
    }

    fn parse_definitions(&mut self, mut stx: Object) -> Result<Object, String> {
        if let Some(matches) = SYNTAX_DEFINITION_PATTERN.try_match(&stx) {
            let Some(MatchValue::One(name_stx)) = matches.get("name") else { panic!() };
            let name = match name_stx.e {
                ObjectT::Symbol(ref name) => name.clone(),
                _ => return Err(format!("define-syntax: expected identifier: {}", name_stx)),
            };

            let Some(MatchValue::One(literals_stx)) = matches.get("literals") else { panic!() };
            let literals = literals_stx.children.iter().map(|stx| match stx.e {
                ObjectT::Symbol(ref literal) => Ok(literal.as_ref()),
                _ => return Err(format!("define-syntax: expected identifier: {}", stx)),
            }).collect::<Result<HashSet<_>, _>>()?;

            let Some(MatchValue::Many(patterns)) = matches.get("pattern") else { panic!() };
            let patterns = patterns.iter().map(|pattern| {
                let MatchValue::One(pattern_stx) = pattern else { panic!() };
                Pattern::new(pattern_stx, &literals)
            }).collect::<Result<Vec<_>, _>>()?;
            let Some(MatchValue::Many(templates)) = matches.get("template") else { panic!() };
            let templates = templates.iter().map(|template| {
                let MatchValue::One(template_stx) = template else { panic!() };
                Template::from(template_stx)
            }).collect::<Result<Vec<_>, _>>()?;

            if patterns.len() == templates.len() {
                let macro_index = self.macros.len();
                if let Some(bindings) = self.bindings.get_mut(name.as_ref()) {
                    bindings
                } else {
                    self.bindings.entry(name.clone()).or_default()
                }.insert(stx.scopes.clone(), Binding::SyntaxTransformer(macro_index));
                self.macros.push(Rules::from(patterns, templates));

                Ok(Object::new(ObjectT::Boolean(true)))
            } else {
                panic!()
            }
        } else if let Some(matches) = VALUE_DEFINITION_PATTERN.try_match(&stx) {
            let Some(MatchValue::One(formals)) = matches.get("formals") else { panic!() };
            for f in &formals.children {
                if let Object { e: ObjectT::Symbol(name), scopes, .. } = f {
                    let b = Binding::Variable(self.new_id());
                    if let Some(bindings) = self.bindings.get_mut(name.as_ref()) {
                        bindings
                    } else {
                        self.bindings.entry(name.clone()).or_default()
                    }.insert(scopes.clone(), b);
                } else {
                    return Err(format!("define-values: not an identifier: {}", f));
                }
            }

            stx.children.first_mut().unwrap().e = ObjectT::Symbol(Cow::Borrowed(CoreForm::SetValues.as_str()));
            Ok(stx)
        } else if let Some(matches) = SYNTAX_BINDING_PATTERN.try_match(&stx) {
            let bind_scope = self.new_scope();
            let body_scopes = stx.scopes.iter().copied().chain(std::iter::once(bind_scope)).collect::<ScopeSet>();
            let Some(MatchValue::Many(names)) = matches.get("name") else { panic!() };
            let names = names.iter().map(|name| {
                let MatchValue::One(name_stx) = name else { panic!() };
                if let ObjectT::Symbol(name) = &(**name_stx).e {
                    Ok(name)
                } else {
                    Err(format!("let-syntax: not an identifier: {}", name_stx))
                }
            }).collect::<Result<Vec<_>, _>>()?;

            let macro_start = self.macros.len();
            for (i, name) in names.iter().enumerate() {
                if let Some(bindings) = self.bindings.get_mut(name.as_ref()) {
                    bindings
                } else {
                    self.bindings.entry(Cow::clone(name)).or_default()
                }.insert(body_scopes.clone(), Binding::SyntaxTransformer(macro_start + i));
            }

            let Some(MatchValue::Many(literals)) = matches.get("literals") else { panic!() };
            let literals = literals.iter().map(|literal| {
                let MatchValue::One(literals_stx) = literal else { panic!() };
                literals_stx.children.iter().map(|literal_stx| {
                    if let ObjectT::Symbol(ref literal) = literal_stx.e {
                        Ok(literal.as_ref())
                    } else {
                        Err(format!("let-syntax: not an identifier: {}", literal_stx))
                    }
                }).collect::<Result<HashSet<_>, _>>()
            }).collect::<Result<Vec<_>, _>>()?;

            let Some(MatchValue::Many(patternses)) = matches.get("pattern") else { panic!() };
            let patternses = patternses.iter().enumerate().map(|(i, patterns)| {
                let MatchValue::Many(patterns) = patterns else { panic!() };
                patterns.iter().map(|pattern| {
                    let MatchValue::One(pattern_stx) = pattern else { panic!() };
                    Pattern::new(pattern_stx, &literals[i])
                }).collect::<Result<Vec<_>, _>>()
            }).collect::<Result<Vec<_>, _>>()?;

            let Some(MatchValue::Many(templateses)) = matches.get("template") else { panic!() };
            let templateses = templateses.iter().map(|templates| {
                let MatchValue::Many(templates) = templates else { panic!() };
                templates.iter().map(|template| {
                    let MatchValue::One(template_stx) = template else { panic!() };
                    Template::from(template_stx)
                }).collect::<Result<Vec<_>, _>>()
            }).collect::<Result<Vec<_>, _>>()?;

            if names.len() == patternses.len() && patternses.len() == templateses.len() {
                self.macros.extend(std::iter::zip(patternses, templateses)
                    .map(|(patterns, templates)| Rules::from(patterns, templates)));

                let Some(MatchValue::Many(bodys)) = matches.get("body") else { panic!() };
                let bodys = bodys.iter().map(|body| {
                    let MatchValue::One(body) = body else { panic!() };
                    let mut body = (**body).clone();
                    body.add_scope(bind_scope);
                    body
                });
                Ok(Object::new(ObjectT::List)
                    .with_children(std::iter::once(Object::new(ObjectT::Symbol(Cow::Borrowed("begin"))))
                        .chain(bodys).collect())
                    .with_scopes(body_scopes))
            } else {
                panic!()
            }
        } else {
            stx.children = stx.children.into_iter()
                .map(|stx| self.parse_definitions(stx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(stx)
        }
    }

    fn expand_all(&mut self, mut stx: Object) -> Result<Object, String> {
        while self.expand_once(&mut stx)? { };
        Ok(stx)
    }
    fn expand_once(&mut self, stx: &mut Object) -> Result<bool, String> {
        match stx.e {
            ObjectT::List | ObjectT::ImproperList => {
                let head = match stx.children.first() {
                    Some(stx) => self.resolve(stx),
                    None => return Ok(false),
                };
                if let Some(Binding::SyntaxTransformer(index)) = head {
                    *stx = self.macros.get(index).unwrap().iter()
                        .find_map(|(pattern, template)| pattern.try_match(stx).map(|e| (e, template)))
                        .map(|(env, template)| template.apply(&MatchSlice::from(&env), stx, self.new_scope()))
                        .unwrap_or(Err(format!("syntax-rules: {}: no matching pattern for: {}", stx.children.first().unwrap(), stx)))?;
                    return Ok(true);
                } else if Some(Binding::CoreForm(CoreForm::Quote)) == head {
                    return Ok(false);
                } else if Some(Binding::CoreForm(CoreForm::Quasiquote)) == head {
                    return self.expand_in_qq(stx.children.get_mut(1).ok_or_else(|| format!("quasiquote: bad syntax"))?, 1);
                }
            },
            ObjectT::Quote => return Ok(false),
            ObjectT::Quasiquote => return self.expand_in_qq(stx.children.first_mut().ok_or_else(|| format!("quasiquote: bad syntax"))?, 1),
            _ => (),
        };

        Ok(stx.children.iter_mut().try_fold(false, |modified, stx| self.expand_once(stx).map(|r| r || modified))?)
    }
    fn expand_in_qq(&mut self, stx: &mut Object, qq: usize) -> Result<bool, String> {
        if qq == 0 {
            return self.expand_once(stx);
        }
        let qq = match stx.e {
            ObjectT::List | ObjectT::ImproperList => match stx.children.first() {
                Some(head_stx) => match self.resolve(head_stx) {
                    Some(Binding::CoreForm(CoreForm::Quasiquote)) => return self.expand_in_qq(stx.children.get_mut(1).ok_or_else(|| format!("quasiquote: bad syntax"))?, qq + 1),
                    Some(Binding::CoreForm(CoreForm::Unquote | CoreForm::UnquoteSplicing)) => return self.expand_in_qq(stx.children.get_mut(1).ok_or_else(|| format!("quasiquote: bad syntax"))?, qq - 1),
                    _ => qq,
                },
                None => return Ok(false),
            },
            ObjectT::Quasiquote => qq + 1,
            ObjectT::Unquote | ObjectT::UnquoteSplicing => qq - 1,
            _ => qq,
        };
        Ok(stx.children.iter_mut().try_fold(false, |modified, stx| self.expand_in_qq(stx, qq).map(|r| r || modified))?)
    }

    fn resolve_syntax(&self, stx: &Object) -> Result<Located<Expression>, String> {
        use ObjectT::*;
        match stx.e {
            Boolean(_) | Integer(_) | Float(_) | Character(_) | String(_) | Bytes(_) | Vector => Ok(Expression::Literal(Literal::from(stx)).at(stx.location)),
            Symbol(ref name) => match self.resolve(stx) {
                None => Err(format!("unbound identifier: {}", name)),
                Some(Binding::CoreForm(_)) => Err(format!("bad syntax: {}", name)),
                Some(Binding::CorePrimitive(name)) => Ok(Expression::Core(name).at(stx.location)),
                Some(Binding::SyntaxTransformer(_)) => panic!(),
                Some(Binding::Variable(x)) => Ok(Expression::Variable(x).at(stx.location)),
            },
            ImproperList | Unquote | UnquoteSplicing => Err(format!("bad syntax: {}", stx)),
            Quote => Ok(Expression::Literal(Literal::from(stx.children.first().ok_or_else(|| format!("bad syntax: {}", stx))?)).at(stx.location)),
            Quasiquote => self.resolve_qq(stx.children.first().ok_or_else(|| format!("bad syntax: {}", stx))?, 1),
            List => match stx.children.first() {
                None => Ok(Expression::Literal(Literal::from(stx)).at(stx.location)),
                Some(head) => match self.resolve(head) {
                    None => if let ObjectT::Symbol(_) = head.e {
                        Err(format!("unbound identifier: {}", head))
                    } else {
                        Ok(Expression::Call {
                            operator: Box::new(self.resolve_syntax(head)?),
                            operands: stx.children.iter().skip(1)
                                .map(|stx| self.resolve_syntax(stx))
                                .collect::<Result<Vec<_>, _>>()?,
                        }.at(stx.location))
                    },
                    Some(Binding::CoreForm(operator)) => self.resolve_core_form_application(operator, stx),
                    Some(Binding::CorePrimitive(core_primitive)) => Ok(Expression::Call {
                        operator: Box::new(Expression::Core(core_primitive).at(head.location)),
                        operands: stx.children.iter().skip(1)
                            .map(|stx| self.resolve_syntax(stx))
                            .collect::<Result<Vec<_>, _>>()?,
                    }.at(stx.location)),
                    Some(Binding::SyntaxTransformer(_)) => panic!(),
                    Some(Binding::Variable(i)) => Ok(Expression::Call {
                        operator: Box::new(Expression::Variable(i).at(head.location)),
                        operands: stx.children.iter().skip(1)
                            .map(|stx| self.resolve_syntax(stx))
                            .collect::<Result<Vec<_>, _>>()?,
                    }.at(stx.location)),
                }
            }
        }
    }
    fn resolve_core_form_application(&self, operator: CoreForm, stx: &Object) -> Result<Located<Expression>, String> {
        use CoreForm::*;
        let create_ids = |stx: &Object| match stx {
            Object { e: ObjectT::Symbol(_), location, .. } => match self.resolve(stx) {
                Some(Binding::Variable(i)) => Ok((Vec::new(), Some(Located::new(i, *location)))),
                Some(_) => Err(format!("bad formals syntax: {}", stx)),
                None => panic!(),
            },
            Object { e: ObjectT::List, children, .. } => {
                let formals = children.iter()
                    .map(|stx| match self.resolve(stx) {
                        Some(Binding::Variable(i)) => Ok(Located::new(i, stx.location)),
                        Some(_) => Err(()),
                        None => panic!(),
                    }).collect::<Result<Vec<_>, _>>().map_err(|_| format!("bad formals syntax: {}", stx))?;
                Ok((formals, None))
            },
            Object { e: ObjectT::ImproperList, children, .. } => {
                let mut formals = children.iter()
                    .map(|stx| match self.resolve(stx) {
                        Some(Binding::Variable(i)) => Ok(Located::new(i, stx.location)),
                        Some(_) => Err(()),
                        None => panic!(),
                    }).collect::<Result<Vec<_>, _>>().map_err(|_| format!("bad formals syntax: {}", stx))?;
                let tail = formals.pop();
                Ok((formals, tail))
            },
            _ => Err(format!("bad formals syntax: {}", stx)),
        };

        match operator {
            Lambda => if stx.children.len() > 2 {
                Ok(Expression::Lambda {
                    formals: create_ids(stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))?)?,
                    body: stx.children.iter().skip(2)
                        .map(|stx| self.resolve_syntax(stx))
                .       collect::<Result<Vec<_>, _>>()?,
                }.at(stx.location))
            } else {
                Err(format!("lambda: bad syntax: {}", stx))
            },
            If => Ok(Expression::If {
                test: Box::new(self.resolve_syntax(stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))?)?),
                consequent: Box::new(self.resolve_syntax(stx.children.get(2).ok_or_else(|| format!("bad syntax: {}", stx))?)?),
                alternate: stx.children.get(3).map(|stx| self.resolve_syntax(stx)).transpose()?.map(Box::new),
            }.at(stx.location)),
            Begin => stx.children.iter().skip(1)
                .map(|stx| self.resolve_syntax(stx))
                .collect::<Result<Vec<_>, _>>()
                .map(|body| Expression::Block { body }.at(stx.location)),
            SetValues => Ok(Expression::Assign {
                ids: create_ids(stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))?)?,
                values: Box::new(self.resolve_syntax(stx.children.get(2).ok_or_else(|| format!("bad syntax: {}", stx))?)?),
            }.at(stx.location)),
            Quote => stx.children.get(1).map(Literal::from).map(Expression::Literal)
                .map(|e| e.at(stx.location))
                .ok_or_else(|| format!("bad syntax: {}", stx)),
            Quasiquote => self.resolve_qq(stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))?, 1),
            Unquote | UnquoteSplicing => Err(format!("unexpected syntax: {}", stx)),
        }
    }
    fn resolve_qq(&self, stx: &Object, depth: usize) -> Result<Located<Expression>, String> {
        use ObjectT::*;
        match stx.e {
            Boolean(_) | Integer(_) | Float(_) | Character(_) | String(_) | Symbol(_) | Bytes(_) | Vector | Quote => Ok(Expression::Literal(Literal::from(stx)).at(stx.location)),
            List => match stx.children.first() {
                None => Ok(Expression::Literal(Literal::from(stx)).at(stx.location)),
                Some(head) => match self.resolve(head) {
                    Some(Binding::CoreForm(CoreForm::Quote)) => Ok(Expression::Literal(Literal::from(stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))?)).at(stx.location)),
                    Some(Binding::CoreForm(CoreForm::Quasiquote)) => stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))
                        .map(|stx| self.resolve_qq(stx, depth + 1))?
                        .map(|e| Expression::Call {
                            operator: Box::new(Expression::Core("list").introduced()),
                            operands: vec![Expression::Literal(LiteralD::Static("quasiquote").into()).introduced(), e],
                        }.at(stx.location)),
                    Some(Binding::CoreForm(CoreForm::Unquote | CoreForm::UnquoteSplicing)) if depth == 1 => self.resolve_syntax(stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))?),
                    Some(Binding::CoreForm(CoreForm::Unquote)) => stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))
                        .map(|stx| self.resolve_qq(stx, depth - 1))?
                        .map(|e| Expression::Call {
                            operator: Box::new(Expression::Core("list").introduced()),
                            operands: vec![Expression::Literal(LiteralD::Static("unquote").into()).introduced(), e],
                        }.at(stx.location)),
                    Some(Binding::CoreForm(CoreForm::UnquoteSplicing)) => stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))
                        .map(|stx| self.resolve_qq(stx, depth - 1))?
                        .map(|e| Expression::Call {
                            operator: Box::new(Expression::Core("list").introduced()),
                            operands: vec![Expression::Literal(LiteralD::Static("unquote-splicing").into()).introduced(), e],
                        }.at(stx.location)),
                    _ => self.resolve_qq_list(stx, depth),
                },
            },
            ImproperList => match self.resolve_qq_list(stx, depth)? {
                // e is either: Literal(List(vec, None)), so we just take the last atom out and stick it in the tail.
                // or ProcedureCall(list, ...), so we take the last one out and say (append [orig] [tail])
                // or ProcedureCall(append, (list ... ) ...), so we find the last child (which will be a list) and do the same
                Located { item: Expression::Literal(Literal::NoCopy(LiteralD::List(mut ee, None))), location } => {
                    let tail = ee.pop().map(Box::new);
                    Ok(Expression::Literal(Literal::NoCopy(LiteralD::List(ee, tail))).at(location))
                },
                Located { item: Expression::Call { operator, mut operands }, location } => match (&**operator, operands.pop()) {
                    (Expression::Core("list"), Some(tail)) => Ok(Expression::Call {
                        operator: Box::new(Expression::Core("append").introduced()),
                        operands: vec![Expression::Call {
                            operator: Box::new(Expression::Core("list").introduced()),
                            operands,
                        }.at(location), tail],
                    }.at(location)),
                    (Expression::Core("list"), None) => Ok(Expression::Call { operator, operands }.at(location)),
                    (Expression::Core("append"), Some(Located { item: Expression::Call { operator: tail_operator, operands: mut tail_operands }, location: tail_location })) if matches!(**tail_operator, Expression::Core("list")) => {
                        if let Some(tail) = tail_operands.pop() {
                            operands.push(Expression::Call { operator: tail_operator, operands: tail_operands }.at(tail_location));
                            operands.push(tail);
                        }
                        Ok(Expression::Call { operator, operands }.at(location))
                    },
                    (Expression::Core("append"), None) => Ok(Expression::Literal(Literal::Copy(LiteralC::Nil)).at(location)),
                    _ => panic!(),
                },
                _ => panic!(),
            },
            Quasiquote => stx.children.first().ok_or_else(|| format!("bad syntax: {}", stx))
                .map(|stx| self.resolve_qq(stx, depth + 1))?
                .map(|e| Expression::Call {
                    operator: Box::new(Expression::Core("list").introduced()),
                    operands: vec![Expression::Literal(LiteralD::Static("quasiquote").into()).introduced(), e],
                }.at(stx.location)),
            Unquote | UnquoteSplicing if depth == 1 => self.resolve_syntax(stx.children.first().ok_or_else(|| format!("bad syntax: {}", stx))?),
            Unquote => stx.children.first().ok_or_else(|| format!("bad syntax: {}", stx))
                .map(|stx| self.resolve_qq(stx, depth - 1))?
                .map(|e| Expression::Call {
                    operator: Box::new(Expression::Core("list").introduced()),
                    operands: vec![Expression::Literal(LiteralD::Static("unquote").into()).introduced(), e],
                }.at(stx.location)),
            UnquoteSplicing => stx.children.first().ok_or_else(|| format!("bad syntax: {}", stx))
                .map(|stx| self.resolve_qq(stx, depth - 1))?
                .map(|e| Expression::Call {
                    operator: Box::new(Expression::Core("list").introduced()),
                    operands: vec![Expression::Literal(LiteralD::Static("unquote-splicing").into()).introduced(), e],
                }.at(stx.location)),
        }
    }
    fn resolve_qq_list(&self, stx: &Object, depth: usize) -> Result<Located<Expression>, String> {
        let mut appending = false;
        let mut blocks = Vec::new();
        for stx in &stx.children {
            let (order, e) = match &stx.e {
                ObjectT::List => {
                    if let Some(ObjectT::Symbol(name)) = stx.children.first().map(|stx| &stx.e) {
                        match name.as_ref() {
                            "unquote" => (ObjectT::Unquote, self.resolve_qq(stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))?, depth - 1)?),
                            "unquote-splicing" => (ObjectT::UnquoteSplicing, self.resolve_qq(stx.children.get(1).ok_or_else(|| format!("bad syntax: {}", stx))?, depth - 1)?),
                            _ => (ObjectT::Unquote, self.resolve_qq(stx, depth)?),
                        }
                    } else {
                        (ObjectT::Unquote, self.resolve_qq(stx, depth)?)
                    }
                },
                ObjectT::UnquoteSplicing => (ObjectT::UnquoteSplicing, self.resolve_qq(stx, depth)?),
                _ => (ObjectT::Unquote, self.resolve_qq(stx, depth)?),
            };

            if order == ObjectT::UnquoteSplicing {
                blocks.push(e);
                appending = false;
            } else if appending {
                let Some(Located { item: Expression::Call { operands, .. }, .. }) = blocks.last_mut() else { panic!() };
                operands.push(e);
            } else {
                blocks.push(Expression::Call {
                    operator: Box::new(Expression::Core("list").introduced()),
                    operands: vec![e],
                }.at(stx.location));
                appending = true;
            }
        }

        match blocks.len() {
            0 => Ok(Expression::Literal(Literal::Copy(LiteralC::Nil)).at(stx.location)),
            1 => match blocks.into_iter().next().unwrap() {
                Located { item: Expression::Call { operands, .. }, location } if operands.iter().all(|e| matches!(&**e, Expression::Literal(_))) => {
                    let literals = operands.into_iter().map(|e| match e {
                        Located { item: Expression::Literal(e), .. } => e,
                        _ => unreachable!(),
                    });
                    Ok(Expression::Literal(Literal::NoCopy(LiteralD::List(literals.collect(), None))).at(location))
                },
                e => Ok(e),
            },
            2.. => Ok(Expression::Call {
                operator: Box::new(Expression::Core("append").introduced()),
                operands: blocks,
            }.at(stx.location)),
        }
    }
}

pub fn expand(stx: Object, env: &mut frontend::Environment) -> Result<Located<Expression>, String> {
    let mut env = Environment::from(env);
    let stx = env.expand_all(stx)?;
    let stx = env.add_binding_scopes(stx)?;
    let stx = env.parse_definitions(stx)?;
    let stx = env.expand_all(stx)?;
    let stx = env.add_binding_scopes(stx)?;
    let e = env.resolve_syntax(&stx)?;

    env.parent_env.bound_variables.extend(env.bindings.iter()
        .map(|(_, b)| b.values())
        .flatten()
        .filter(|b| matches!(b, Binding::Variable(_))));
    env.parent_env.toplevels.extend(env.bindings.into_iter()
        .filter_map(|(name, bindings)| bindings.get(&ScopeSet::default()).map(|b| (name, *b))));
    Ok(e)
}

/******** parser ********/

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Identifier(String),
    Boolean(bool),
    Integer(String),
    Decimal(String),
    InfNaN(String),
    Character(String),
    String(String),
    Open,
    Close,
    Vector,
    Bytes,
    Quote,
    Backquote,
    Unquote,
    UnquoteSplicing,
    Dot,
    Garbage,
}

pub fn read(port: &mut InputPort) -> Result<Object, &'static str> {
    read_datum(port, None)
}

fn read_datum(port: &mut InputPort, token: Option<(Token, Location)>) -> Result<Object, &'static str> {
    use Token::*;
    let (token, location) = token.unwrap_or_else(|| next_token(port).unwrap());
    match token {
        Identifier(s) => parse_identifier(&s).map(Cow::Owned).map(ObjectT::Symbol)
            .map(|e| Object::new(e).with_source(location)),
        Boolean(s) => Ok(Object::new(ObjectT::Boolean(s)).with_source(location)),
        Integer(_) | Decimal(_) | InfNaN(_) => parse_number(token).map(|e| Object::new(e).with_source(location)),
        Character(s) => parse_character(&s).map(ObjectT::Character).map(|e| Object::new(e).with_source(location)),
        String(s) => parse_string(&s).map(ObjectT::String).map(|e| Object::new(e).with_source(location)),
        Open => {
            let mut children = Vec::new();
            loop {
                match next_token(port).unwrap() {
                    (Close, _) => break Ok((ObjectT::List, children)),
                    (Dot, _) => {
                        children.push(read_datum(port, None)?);
                        match next_token(port).unwrap() {
                            (Close, _) => break Ok((ObjectT::ImproperList, children)),
                            _ => break Err("unexpected token"),
                        }
                    },
                    token => children.push(read_datum(port, Some(token))?),
                }
            }.map(|(ty, children)| Object::new(ty).with_children(children).with_source(location))
        },
        Vector => {
            let mut children = Vec::new();
            loop {
                match next_token(port).unwrap() {
                    (Close, _) => break Ok(children),
                    token => children.push(read_datum(port, Some(token))?),
                }
            }.map(|children| Object::new(ObjectT::Vector).with_children(children).with_source(location))
        },
        Bytes => {
            let mut bytes = Vec::new();
            let bytes = loop {
                match next_token(port).unwrap() {
                    (Close, _) => break bytes,
                    (Token::Integer(byte), _) => {
                        let s = byte.as_str();
                        let (radix, s) = match &s[0 .. 2] {
                            "#b" => (2, &s[2 ..]),
                            "#o" => (8, &s[2 ..]),
                            "#x" => (10, &s[2 ..]),
                            "#d" => (16, &s[2 ..]),
                            _ => (10, s),
                        };
                        let src = match s.bytes().next().unwrap() {
                            b'+' => &s[1 ..],
                            b'-' => return Err("invalid byte in bytevector"),
                            _ => s,
                        };
                        bytes.push(match u8::from_str_radix(src, radix) {
                            Ok(n) => n,
                            Err(e) if *e.kind() == std::num::IntErrorKind::Zero => 0,
                            _ => return Err("invalid byte in bytevector"),
                        });
                    },
                    _ => return Err("unexpected token"),
                }
            };
            Ok(Object::new(ObjectT::Bytes(bytes)).with_source(location))
        }
        Quote => Ok(Object::new(ObjectT::Quote).with_children(vec![read_datum(port, None)?]).with_source(location)),
        Backquote => Ok(Object::new(ObjectT::Quasiquote).with_children(vec![read_datum(port, None)?]).with_source(location)),
        Unquote => Ok(Object::new(ObjectT::Unquote).with_children(vec![read_datum(port, None)?]).with_source(location)),
        UnquoteSplicing => Ok(Object::new(ObjectT::UnquoteSplicing).with_children(vec![read_datum(port, None)?]).with_source(location)),
        Close | Dot | Garbage => Err("unexpected token"),
    }
}
fn parse_boolean(s: &str) -> bool {
    match s {
        "#t" | "#true" => true,
        "#f" | "#false" => false,
        _ => unreachable!(),
    }
}
fn parse_number(t: Token) -> Result<ObjectT, &'static str> {
    let s = match &t {
        Token::Integer(s) | Token::Decimal(s) | Token::InfNaN(s) => s.as_str(),
        _ => panic!(),
    };
    let (radix, s) = if s.len() >= 2 {
        match &s[0 .. 2] {
            "#b" => (2, &s[2 ..]),
            "#o" => (8, &s[2 ..]),
            "#x" => (10, &s[2 ..]),
            "#d" => (16, &s[2 ..]),
            _ => (10, s),
        }
    } else {
        (10, s)
    };
    let (sign, s) = match s.bytes().next().unwrap() {
        b'+' => (1, &s[1 ..]),
        b'-' => (-1, &s[1 ..]),
        _ => (1, s),
    };
    let try_parse_flt = |s: &str| s.parse::<f32>()
        .map(ObjectT::Float)
        .map_err(|_| "illegal floating-point number");
    let try_parse_int = |src: &str| match i64::from_str_radix(src, radix) {
        Ok(n) => Ok(ObjectT::Integer(sign as i64 * n)),
        Err(e) => match e.kind() {
            std::num::IntErrorKind::Zero => Ok(ObjectT::Integer(0)),
            std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => try_parse_flt(src),
            _ => panic!(),
        },
    };

    match t {
        Token::Integer(_) => try_parse_int(s),
        Token::Decimal(_) => try_parse_flt(s),
        Token::InfNaN(_) => match &s[0 .. 3] {
            "inf" => Ok(ObjectT::Float(sign as f32 * f32::INFINITY)),
            "nan" => Ok(ObjectT::Float(sign as f32 * f32::NAN)),
            _ => panic!(),
        },
        _ => unreachable!(),
    }
}
fn parse_character(s: &str) -> Result<char, &'static str> {
    match s {
        "#\\alarm" => Ok('\u{0007}'),
        "#\\backspace" => Ok('\u{0008}'),
        "#\\delete" => Ok('\u{007F}'),
        "#\\escape" => Ok('\u{001B}'),
        "#\\newline" => Ok('\n'),
        "#\\return" => Ok('\r'),
        "#\\space" => Ok(' '),
        "#\\tab" => Ok('\t'),
        _ => {
            match s.chars().skip(2).next().unwrap() {
                'x' => {
                    u32::from_str_radix(&s[3..], 16)
                        .ok()
                        .and_then(char::from_u32)
                        .ok_or("invalid character")
                },
                c => Ok(c),
            }
        }
    }
}
fn parse_string(s: &str) -> Result<String, &'static str> {
    let mut chars = s.chars().skip(1).peekable();
    let mut parsed = String::with_capacity(s.len() - 2);
    while let Some(c) = chars.next() {
        match c {
            '"' => return Ok(parsed),
            '\\' => {
                match chars.next().ok_or("unterminated escaped string element")? {
                    'a' => parsed.push('\u{0007}'),
                    'b' => parsed.push('\u{0008}'),
                    'n' => parsed.push('\n'),
                    'r' => parsed.push('\r'),
                    't' => parsed.push('\t'),
                    '"' => parsed.push('"'),
                    '\\' => parsed.push('\\'),
                    'x' => {
                        let mut ccode = 0;
                        while let Some(digit) = chars.next_if(|c| c.is_digit(16)) {
                            ccode = ccode * 16 + digit.to_digit(16).unwrap();
                        }
                        if chars.next().unwrap() == ';' {
                            parsed.push(unsafe { char::from_u32_unchecked(ccode)});
                        }
                    },
                    ' ' | '\t' | '\n' | '\r' => {
                        while let Some(c) = chars.next() {
                            if c != ' ' && c != '\t' {
                                break;
                            }
                        }
                        match chars.next().ok_or("unterminated escaped whitespace")? {
                            '\n' => parsed.push('\n'),
                            '\r' => {
                                parsed.push('\r');
                                if chars.peek().is_some_and(|&c| c == '\n') {
                                    parsed.push('\n');
                                    chars.next();
                                }
                            },
                            _ => return Err("illegally terminated escaped whitespace"),
                        }
                        while let Some(c) = chars.next() {
                            if c != ' ' && c != '\t' {
                                break;
                            }
                        }
                    },
                    _ => return Err("invalid escaped string element"),
                };
            },
            _ => parsed.push(c),
        }
    }
    Err("invalid string element")
}
fn parse_identifier(s: &str) -> Result<String, &'static str> {
    if s.starts_with('|') {
        let mut chars = s.chars().skip(1).peekable();
        let mut parsed = String::new();
        while let Some(c) = chars.next() {
            match c {
                '|' => return Ok(parsed),
                '\\' => {
                    match chars.next().ok_or("unterminated escaped symbol element")? {
                        'a' => parsed.push('\u{0007}'),
                        'b' => parsed.push('\u{0008}'),
                        'n' => parsed.push('\n'),
                        'r' => parsed.push('\r'),
                        't' => parsed.push('\t'),
                        '|' => parsed.push('|'),
                        '\\' => parsed.push('\\'),
                        'x' => {
                            let mut ccode = 0;
                            while let Some(digit) = chars.next_if(|c| c.is_digit(16)) {
                                ccode = ccode * 16 + digit.to_digit(16).unwrap();
                            }
                            if chars.next().unwrap() == ';' {
                                parsed.push(unsafe { char::from_u32_unchecked(ccode)});
                            }
                        },
                        _ => return Err("invalid character"),
                    };
                },
                _ => parsed.push(c),
            }
        }
        Err("invalid symbol element")
    } else {
        Ok(s.to_string())
    }
}

fn next_token(port: &mut InputPort) -> Result<(Token, Location), std::io::Error> {
    while let Some(dh) = atmosphere(port, 0)? {
        port.advance(dh);
    }

    let source_location = Location::from(port.location());
    if let Some(r) = boolean(port, 0)? {
        if r > 0 {
            return Ok((Token::Boolean(parse_boolean(port.advance(r))), source_location));
        }
    }
    if let Some(r) = number_decimal(port, 0)? {
        if r > 0 {
            return Ok((Token::Decimal(port.advance(r).to_string()), source_location));
        }
    }
    if let Some(r) = number_integer(port, 0)? {
        if r > 0 {
            return Ok((Token::Integer(port.advance(r).to_string()), source_location));
        }
    }
    if let Some(r) = number_infnan(port, 0)? {
        if r > 0 {
            return Ok((Token::InfNaN(port.advance(r).to_string()), source_location));
        }
    }
    if let Some(r) = character(port, 0)? {
        if r > 0 {
            return Ok((Token::Character(port.advance(r).to_string()), source_location));
        }
    }
    if let Some(r) = string(port, 0)? {
        if r > 0 {
            return Ok((Token::String(port.advance(r).to_string()), source_location));
        }
    }
    if let Some(r) = identifier(port, 0)? {
        if r > 0 {
            return Ok((Token::Identifier(port.advance(r).to_string()), source_location));
        }
    }
    if port.test("(", 0)? {
        port.advance(1);
        return Ok((Token::Open, source_location));
    }
    if port.test(")", 0)? {
        port.advance(1);
        return Ok((Token::Close, source_location));
    }
    if port.test("#(", 0)? {
        port.advance(2);
        return Ok((Token::Vector, source_location));
    }
    if port.test("#u8(", 0)? {
        port.advance(4);
        return Ok((Token::Bytes, source_location));
    }
    if port.test("'", 0)? {
        port.advance(1);
        return Ok((Token::Quote, source_location));
    }
    if port.test("`", 0)? {
        port.advance(1);
        return Ok((Token::Backquote, source_location));
    }
    if port.test(",@", 0)? {
        port.advance(2);
        return Ok((Token::UnquoteSplicing, source_location));
    }
    if port.test(",", 0)? {
        port.advance(1);
        return Ok((Token::Unquote, source_location));
    }
    if port.test(".", 0)? {
        if let Some(_) = delimiter(port, 1)? {
            port.advance(1);
            return Ok((Token::Dot, source_location));
        }
    }

    Ok((Token::Garbage, source_location))
}
fn delimiter(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = whitespace(port, offset)? {
        Ok(Some(r))
    } else {
        match port.peek(offset)? {
            Some(b'(') | Some(b')') | Some(b'"') | Some(b';') | Some(b'|') => Ok(Some(1)),
            _ => Ok(None),
        }
    }
}
fn intraline_whitespace(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if port.test(" ", offset)? || port.test("\t", offset)? {
        Ok(Some(1))
    } else {
        Ok(None)
    }
}
fn whitespace(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = intraline_whitespace(port, offset)? {
        Ok(Some(r))
    } else if let Some(r) = line_ending(port, offset)? {
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn line_ending(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if port.test("\n", offset)? {
        Ok(Some(1))
    } else if port.test("\r\n", offset)? {
        Ok(Some(2))
    } else if port.test("\r", offset)? {
        Ok(Some(1))
    } else {
        Ok(None)
    }
}
fn comment(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = nested_comment(port, offset)? {
        Ok(Some(r))
    } else if port.peek(offset)? == Some(b';') {
        let mut r = 1;
        while line_ending(port, offset + r)?.is_none() {
            r += 1;
        }
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn nested_comment(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if port.test("#|", offset)? {
        let mut r = 2 + comment_text(port, offset + 2)?.unwrap_or(0);
        while let Some(dr) = comment_cont(port, offset + r)? {
            r += dr;
            if dr == 0 {
                break;
            }
        }
        if port.test("|#", offset + r)? {
            return Ok(Some(r + 2));
        }
    }
    Ok(None)
}
fn comment_text(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    let mut r = 0;
    while !port.test("#|", offset + r)? && !port.test("|#", offset + r)? {
        r += 1;
    }
    Ok(Some(r))
}
fn comment_cont(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = nested_comment(port, offset)? {
        Ok(Some(r + comment_text(port, offset + r)?.unwrap_or(0)))
    } else {
        Ok(None)
    }
}
fn atmosphere(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = comment(port, offset)? {
        Ok(Some(r))
    } else if let Some(r) = whitespace(port, offset)? {
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn identifier(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(mut r) = initial(port, offset)? {
        while let Some(dr) = subsequent(port, offset + r)? {
            r += dr;
        }
        if let Some(_) = delimiter(port, offset + r)? {
            return Ok(Some(r));
        }
    }
    if let Some(r) = peculiar_identifier(port, offset)? {
        return Ok(Some(r));
    }
    if port.test("|", offset)? {
        let mut r = 1;
        while let Some(dr) = symbol_element(port, offset + r)? {
            r += dr;
        }
        if port.test("|", offset + r)? {
            return Ok(Some(r + 1));
        }
    }
    Ok(None)
}
fn initial(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    match port.peek(offset)? {
        Some(b'a'..=b'z') | Some(b'A'..=b'Z') => Ok(Some(1)),
        Some(b'!') | Some(b'$') | Some(b'%') | Some(b'&') => Ok(Some(1)),
        Some(b'*') | Some(b'/') | Some(b':') | Some(b'<') => Ok(Some(1)),
        Some(b'=') | Some(b'>') | Some(b'?') | Some(b'^') => Ok(Some(1)),
        Some(b'_') | Some(b'~') => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn subsequent(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = initial(port, offset)? {
        return Ok(Some(r));
    }
    if let Some(r) = digit(port, offset, 10)? {
        return Ok(Some(r));
    }
    match port.peek(offset)? {
        Some(b'+') | Some(b'-') | Some(b'.') | Some(b'@') => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn digit(port: &mut InputPort, offset: usize, base: i32) -> std::io::Result<Option<usize>> {
    match port.peek(offset)? {
        Some(b'0'..=b'1') if base >= 2 => Ok(Some(1)),
        Some(b'0'..=b'7') if base >= 8 => Ok(Some(1)),
        Some(b'0'..=b'9') if base >= 10 => Ok(Some(1)),
        Some(b'a'..=b'f') if base >= 16 => Ok(Some(1)),
        Some(b'A'..=b'F') if base >= 16 => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn sign(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    match port.peek(offset)? {
        Some(b'+') | Some(b'-') => Ok(Some(1)),
        _ => Ok(Some(0)),
    }
}
fn inline_hex_escape(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if port.test("\\x", offset)? {
        let mut r = 2;
        while let Some(dr) = digit(port, offset + r, 16)? {
            r += dr;
        }
        if port.test(";", offset + r)? {
            return Ok(Some(r + 1));
        }
    }
    Ok(None)
}
fn mnemonic_escape(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    match port.peek(offset)? {
        Some(b'\\') => match port.peek(offset + 1)? {
            Some(b'a') => Ok(Some(1)),
            Some(b'b') => Ok(Some(1)),
            Some(b'n') => Ok(Some(1)),
            Some(b'r') => Ok(Some(1)),
            Some(b't') => Ok(Some(1)),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}
fn peculiar_identifier(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    let r = sign(port, offset)?.unwrap();
    if port.peek(offset + r)? == Some(b'.') {
        if let Some(mut s) = dot_subsequent(port, offset + r + 1)? {
            while let Some(ds) = subsequent(port, offset + r + 1 + s)? {
                s += ds;
            }
            return Ok(Some(r + 1 + s));
        }
    }
    if r > 0 {
        if let Some(mut s) = sign_subsequent(port, offset + r)? {
            while let Some(ds) = subsequent(port, offset + r + s)? {
                s += ds;
            }
            return Ok(Some(r + s));
        } else {
            return Ok(Some(r));
        }
    }
    Ok(None)
}
fn dot_subsequent(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = sign_subsequent(port, offset)? {
        Ok(Some(r))
    } else if port.peek(offset)? == Some(b'.') {
        Ok(Some(1))
    } else {
        Ok(None)
    }
}
fn sign_subsequent(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = initial(port, offset)? {
        return Ok(Some(r));
    }
    match port.peek(offset)? {
        Some(b'+') | Some(b'-') | Some(b'@') => Ok(Some(1)),
        _ => Ok(None),
    }
}
fn symbol_element(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = inline_hex_escape(port, offset)? {
        Ok(Some(r))
    } else if let Some(r) = mnemonic_escape(port, offset)? {
        Ok(Some(r))
    } else {
        match port.peek_char(offset)? {
            Some('|') => Ok(None),
            Some('\\') => {
                match port.peek(offset + 1)? {
                    Some(b'|') => Ok(Some(2)),
                    _ => Ok(None),
                }
            },
            Some(c) => Ok(Some(c.len_utf8())),
            None => Ok(None),
        }
    }
}
fn boolean(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    match if port.test("#true", offset)? {
        Some(5)
    } else if port.test("#false", offset)? {
        Some(6)
    } else if port.test("#t", offset)? || port.test("#f", offset)? {
        Some(2)
    } else {
        None
    }
    {
        Some(r) => {
            if let Some(_) = delimiter(port, offset + r)? {
                Ok(Some(r))
            } else {
                Ok(None)
            }
        },
        None => Ok(None),
    }
}
fn character(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if port.test("#\\", offset)? {
        let r = if port.test("alarm", offset + 2)? {
            Some(7)
        } else if port.test("backspace", offset + 2)? {
            Some(11)
        } else if port.test("delete", offset + 2)? {
            Some(8)
        } else if port.test("escape", offset + 2)? {
            Some(8)
        } else if port.test("newline", offset + 2)? {
            Some(9)
        } else if port.test("return", offset + 2)? {
            Some(8)
        } else if port.test("space", offset + 2)? {
            Some(7)
        } else if port.test("tab", offset + 2)? {
            Some(7)
        } else if port.test("x", offset + 2)? {
            let mut r = 3;
            while let Some(dr) = digit(port, offset + r, 16)? {
                r += dr;
            }
            Some(r)
        } else {
            port.peek_char(offset + 2)?.map(|c| 2 + c.len_utf8())
        };

        if r.is_some_and(|r| r > 0) {
            if delimiter(port, offset + r.unwrap())?.is_some() {
                return Ok(r);
            }
        }
    }
    Ok(None)
}
fn string(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if port.peek(offset)? == Some(b'"'){
        let mut r = 1;
        while let Some(dr) = string_element(port, offset + r)? {
            r += dr;
        }
        if port.peek(offset + r)? == Some(b'"') {
            return Ok(Some(r + 1));
        }
    }
    Ok(None)
}
fn string_element(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = inline_hex_escape(port, offset)? {
        return Ok(Some(r));
    }
    if let Some(r) = mnemonic_escape(port, offset)? {
        return Ok(Some(r));
    }
    if let Some(c) = port.peek_char(offset)? {
        if c == '\\' {
            let nc = port.peek(offset + 1)?;
            if nc == Some(b'"') || nc == Some(b'\\') {
                return Ok(Some(2));
            }

            let mut r = 1;
            while let Some(dr) = intraline_whitespace(port, offset + r)? {
                r += dr;
            }
            if let Some(dr) = line_ending(port, offset + r)? {
                r += dr;
                while let Some(dr) = intraline_whitespace(port, offset + r)? {
                    r += dr;
                }
                return Ok(Some(r));
            }
        } else if c != '"' {
            return Ok(Some(c.len_utf8()));
        }
    }
    Ok(None)
}
fn number_decimal(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(r) = radix(port, offset, 10)? {
        if let Some(s) = sign(port, offset + r)? {
            if let Some(t) = inner_decimal(port, offset + r + s)? {
                if let Some(_) = delimiter(port, offset + r + s + t)? {
                    return Ok(Some(r + s + t));
                }
            }
        }
    }
    Ok(None)
}
fn number_integer(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    for base in [2, 8, 10, 16] {
        if let Some(r) = radix(port, offset, base)? {
            if let Some(s) = sign(port, offset)? {
                if let Some(t) = uinteger(port, offset, base)? {
                    if let Some(_) = delimiter(port, offset + r + s + t)? {
                        return Ok(Some(r + s + t));
                    }
                }
            }
        }
    }
    Ok(None)
}
fn number_infnan(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    for base in [2, 8, 10, 16] {
        if let Some(r) = radix(port, offset, base)? {
            if port.test("+inf.0", offset + r)? || port.test("-inf.0", offset + r)? {
                return Ok(Some(r + 6));
            } else if port.test("+nan.0", offset + r)? || port.test("-nan.0", offset + r)? {
                return Ok(Some(r + 6));
            }
        }
    }
    Ok(None)
}
fn inner_decimal(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    if let Some(mut r) = uinteger(port, offset, 10)? {
        if port.peek(offset + r)? == Some(b'.') {
            r += 1;
            while let Some(dr) = digit(port, offset + r, 10)? {
                r += dr;
            }
            if let Some(s) = suffix(port, offset + r)? {
                return Ok(Some(r + s));
            }
        } else if let Some(s) = suffix(port, offset + r)? {
            if s > 0 {
                return Ok(Some(r + s));
            }
        }
    } else if port.peek(offset)? == Some(b'.') {
        let mut r = 1;
        while let Some(dr) = digit(port, offset + r, 10)? {
            r += dr;
        }
        if r > 1 {
            if let Some(s) = suffix(port, offset + r)? {
                return Ok(Some(r + s));
            }
        }
    }
    Ok(None)
}
fn uinteger(port: &mut InputPort, offset: usize, base: i32) -> std::io::Result<Option<usize>> {
    if let Some(mut r) = digit(port, offset, base)? {
        while let Some(dr) = digit(port, offset + r, base)? {
            r += dr;
        }
        Ok(Some(r))
    } else {
        Ok(None)
    }
}
fn suffix(port: &mut InputPort, offset: usize) -> std::io::Result<Option<usize>> {
    match port.peek(offset)? {
        Some(b'e') | Some(b'E') => {
            if let Some(r) = sign(port, offset + 1)? {
                if let Some(s) = uinteger(port, offset + r + 1, 10)? {
                    return Ok(Some(r + s + 1));
                }
            }
            Ok(None)
        },
        _ => Ok(Some(0)),
    }
}
fn radix(port: &mut InputPort, offset: usize, base: i32) -> std::io::Result<Option<usize>> {
    if base == 2 && port.test("#b", offset)? {
        Ok(Some(2))
    } else if base == 8 && port.test("#b", offset)? {
        Ok(Some(2))
    } else if base == 10 {
        if port.test("#d", offset)? {
            Ok(Some(2))
        } else {
            Ok(Some(0))
        }
    } else if base == 16 && port.test("#x", offset)? {
        Ok(Some(2))
    } else {
        Ok(None)
    }
}

/******** built-in macros ********/

pub(super) fn core_macros() -> (Vec<&'static str>, Vec<Rules>) {
    [
        ("and", Rules(vec![
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("and"))]),
                Template::Constant(Object::new(ObjectT::Boolean(true))),
            ),
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("and")), Pattern::Variable(Cow::Borrowed("test"))]),
                Template::Identifier(Cow::Borrowed("test")),
            ),
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("and")), Pattern::Variable(Cow::Borrowed("test1")), Pattern::Ellipsis(Box::new(Pattern::Variable(Cow::Borrowed("test2"))))]),
                Template::List(vec![
                    Template::Identifier(Cow::Borrowed("if")),
                    Template::Identifier(Cow::Borrowed("test1")),
                    Template::List(vec![
                        Template::Identifier(Cow::Borrowed("and")),
                        Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("test2")))),
                    ]),
                    Template::Constant(Object::new(ObjectT::Boolean(false))),
                ]),
            ),
        ])),
        ("or", Rules(vec![
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("or"))]),
                Template::Constant(Object::new(ObjectT::Boolean(false))),
            ),
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("or")), Pattern::Variable(Cow::Borrowed("test"))]),
                Template::Identifier(Cow::Borrowed("test")),
            ),
            (
                Pattern::List(vec![Pattern::Literal(Cow::Borrowed("or")), Pattern::Variable(Cow::Borrowed("test1")), Pattern::Ellipsis(Box::new(Pattern::Variable(Cow::Borrowed("test2"))))]),
                Template::List(vec![
                    Template::Identifier(Cow::Borrowed("let")),
                    Template::List(vec![Template::List(vec![Template::Identifier(Cow::Borrowed("x")), Template::Identifier(Cow::Borrowed("test1"))])]),
                    Template::List(vec![
                        Template::Identifier(Cow::Borrowed("if")),
                        Template::Identifier(Cow::Borrowed("x")),
                        Template::Identifier(Cow::Borrowed("x")),
                        Template::List(vec![
                            Template::Identifier(Cow::Borrowed("or")),
                            Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("test2")))),
                        ]),
                    ]),
                ]),
            ),
        ])),
        ("let", Rules(vec![
            (
                Pattern::List(vec![
                    Pattern::Literal(Cow::Borrowed("let")),
                    Pattern::List(vec![Pattern::Ellipsis(Box::new(Pattern::List(vec![Pattern::Variable(Cow::Borrowed("name")), Pattern::Variable(Cow::Borrowed("val"))])))]),
                    Pattern::Variable(Cow::Borrowed("body1")),
                    Pattern::Ellipsis(Box::new(Pattern::Variable(Cow::Borrowed("body2")))),
                ]),
                Template::List(vec![
                    Template::List(vec![
                        Template::Identifier(Cow::Borrowed("lambda")),
                        Template::List(vec![Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("name"))))]),
                        Template::Identifier(Cow::Borrowed("body1")),
                        Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("body2")))),
                    ]),
                    Template::Ellipsis(Box::new(Template::Identifier(Cow::Borrowed("val")))),
                ]),
            )
        ])),
    ].into_iter().collect()
}

static SYNTAX_DEFINITION_PATTERN: std::sync::LazyLock<Pattern> = std::sync::LazyLock::new(|| Pattern::List(vec![
    Pattern::Literal(Cow::Borrowed("define-syntax")),
    Pattern::Variable(Cow::Borrowed("name")),
    Pattern::List(vec![
        Pattern::Literal(Cow::Borrowed("syntax-rules")),
        Pattern::Variable(Cow::Borrowed("literals")),
        Pattern::Ellipsis(Box::new(Pattern::List(vec![
            Pattern::Variable(Cow::Borrowed("pattern")),
            Pattern::Variable(Cow::Borrowed("template")),
        ]))),
    ]),
]));
static VALUE_DEFINITION_PATTERN: std::sync::LazyLock<Pattern> = std::sync::LazyLock::new(|| Pattern::List(vec![
    Pattern::Literal(Cow::Borrowed("define-values")),
    Pattern::Variable(Cow::Borrowed("formals")),
    Pattern::Variable(Cow::Borrowed("expression")),
]));
static SYNTAX_BINDING_PATTERN: std::sync::LazyLock<Pattern> = std::sync::LazyLock::new(|| Pattern::ImproperList(vec![
    Pattern::Literal(Cow::Borrowed("letrec-syntax")),
    Pattern::List(vec![
        Pattern::Ellipsis(Box::new(Pattern::List(vec![
            Pattern::Variable(Cow::Borrowed("name")),
            Pattern::List(vec![
                Pattern::Literal(Cow::Borrowed("syntax-rules")),
                Pattern::Variable(Cow::Borrowed("literals")),
                Pattern::Ellipsis(Box::new(Pattern::List(vec![
                    Pattern::Variable(Cow::Borrowed("pattern")),
                    Pattern::Variable(Cow::Borrowed("template")),
                ]))),
            ]),
        ]))),
    ]),
], Box::new(Pattern::Variable(Cow::Borrowed("body")))));
