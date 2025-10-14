#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Arity {
    Exact(usize),
    AtLeast(usize),
    #[default] Unknown,
}
impl Arity {
    pub fn join(self, other: Self) -> Self {
        use Arity::*;
        match (self, other) {
            (Exact(m), Exact(n)) if m == n => Exact(m),
            (Exact(m), Exact(n)) | (Exact(m), AtLeast(n)) | (AtLeast(m), Exact(n)) | (AtLeast(m), AtLeast(n)) => AtLeast(std::cmp::min(m, n)),
            _ => Unknown,
        }
    }
    pub fn compatible_with(self, other: Self) -> bool {
        use Arity::*;
        match (self, other) {
            (Exact(m), Exact(n)) => m == n,
            (AtLeast(m), Exact(n) | AtLeast(n)) => m <= n,
            (Unknown, _) => true,
            _ => false,
        }
    }
}

pub(crate) static PRIMITIVES: std::sync::LazyLock<std::collections::HashMap<&'static str, (Arity, Arity)>> = std::sync::LazyLock::new(|| { use Arity::*; [
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
].into() });
