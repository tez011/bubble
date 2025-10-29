use std::borrow::Cow;
use std::collections::HashMap;
use super::{Arity, Binding, ValueID};
use super::syntax;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CoreForm {
    Lambda,
    If,
    Begin,
    SetValues,
    Quote,
    Quasiquote,
    Unquote,
    UnquoteSplicing,
}
static CORE_FORM_ALL: [CoreForm; 8] = {
    use CoreForm::*;
    [
        Lambda,
        If,
        Begin,
        SetValues,
        Quote,
        Quasiquote,
        Unquote,
        UnquoteSplicing,
    ]
};
impl CoreForm {
    pub(super) fn all() -> [CoreForm; 8] { CORE_FORM_ALL }
    pub(super) fn as_str(self) -> &'static str {
        match self {
            CoreForm::Lambda => "lambda",
            CoreForm::If => "if",
            CoreForm::Begin => "begin",
            CoreForm::SetValues => "set-values!",
            CoreForm::Quote => "quote",
            CoreForm::Quasiquote => "quasiquote",
            CoreForm::Unquote => "unquote",
            CoreForm::UnquoteSplicing => "unquote-splicing",
        }
    }
}

#[derive(Debug)]
pub struct Environment {
    pub macros: Vec<syntax::Rules>,
    pub toplevels: HashMap<Cow<'static, str>, Binding>,
    pub(super) arities: HashMap<Binding, (Arity, Arity)>,
    pub(super) bound_id_counter: std::cell::Cell<usize>,
    pub(super) literals: HashMap<ValueID, std::rc::Rc<super::LiteralD>>,
}
impl Environment {
    pub fn new() -> Self {
        let (core_macro_names, core_macros) = syntax::core_macros();
        let core_primitives = Self::core_primitives();
        Self {
            macros: Vec::from(core_macros),
            toplevels: CoreForm::all().iter().copied()
                .map(|cf| (Cow::Borrowed(cf.as_str()), Binding::CoreForm(cf)))
                .chain(core_primitives.iter().copied().map(|(name, _)| (Cow::Borrowed(name), Binding::CorePrimitive(name))))
                .chain(core_macro_names.iter().copied().enumerate().map(|(i, s)| (Cow::Borrowed(s), Binding::SyntaxTransformer(i))))
                .collect(),
            arities: core_primitives.iter().copied()
                .map(|(name, arity)| (Binding::CorePrimitive(name), arity))
                .collect(),
            bound_id_counter: std::cell::Cell::new(1),
            literals: Default::default(),
        }
    }

    pub(super) fn new_id(&self) -> usize {
        let binding = self.bound_id_counter.get();
        self.bound_id_counter.set(binding.wrapping_add(1));
        binding
    }

    fn core_primitives() -> Vec<(&'static str, (super::Arity, super::Arity))> {
        use super::Arity::*;
        vec![
            ("+", (AtLeast(0), Exact(1))),
            ("-", (AtLeast(1), Exact(1))),
            ("*", (AtLeast(0), Exact(1))),
            ("=", (AtLeast(1), Exact(1))),
            ("list", (AtLeast(0), Exact(1))),
            ("cons", (Exact(2), Exact(1))),
            ("append", (AtLeast(0), Exact(1))),
            ("apply", (AtLeast(2), Unknown)),
            ("call/cc", (Exact(1), Unknown)),
            ("values", (AtLeast(0), Unknown)),
            ("call-with-values", (Exact(2), Unknown)),
            ("dynamic-wind", (Exact(3), Unknown)),
            ("with-exception-handler", (Exact(2), Unknown)),
            ("raise", (Exact(1), Unknown)),
            ("raise-continuable", (Exact(1), Unknown)),
            ("__store_dynamic_extent", (Exact(0), Exact(1))),
            ("__rewind_dynamic_extent", (Exact(1), Exact(0))),
            ("__push_dynamic_frame", (Exact(2), Exact(1))),
            ("__push_exception_frame", (Exact(1), Exact(1))),
            ("__push_parameter_frame", (Exact(2), Exact(1))),
            ("__find_exception_handler", (Exact(0), Exact(2))),
            ("__debug_dump_macros", (Exact(0), Exact(0))),
        ]
    }
}
