//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! A `#[dynamic(lazy, drop)]` static is FINALIZED while other threads still run,
//! and reading one then is a PANIC.
//!
//! `static_init` registers a destructor for every `#[dynamic(lazy, drop)]`
//! static. At process exit those destructors run, and the static enters the
//! `FINALIZED` phase. The generated `.read()` accessor is
//! `init_then_try_read_lock(..).unwrap()` (`generic_lazy.rs`), so a read after
//! finalization does not fail -- it panics.
//!
//! Nothing stops zenoh's own receive and publish threads before that point.
//! They are owned by the runtime, not by these statics, so a thread mapping or
//! allocating a buffer during exit reaches a finalized global. Measured on giedi
//! 2026-11-17 and 2026-11-18: three occurrences, each one second after a
//! deliberate non-zero exit, `thread 'rx-1' panicked ... AccessError { phase:
//! INITIALIZED | REGISTERED | FINALIZED | INITIALIZED_AND_REGISTERED }`. The
//! exit's own error message was good; the panic landed after it and pointed into
//! this crate instead, so the real cause read as a footnote.
//!
//! THIS IS A SOURCE SCAN, and deliberately so. The condition it rules out is a
//! PANIC during process teardown, which no behavioural test in this crate can
//! reach for every site: finalization happens once, after the harness has
//! finished. What can be checked is that no production site holds the panicking
//! accessor, which is the property that keeps the bug from coming back by a route
//! nobody is watching.
//!
//! It PARSES the sources with `syn`. It matched their text one line at a time
//! until BER-2486, and passed with `ShmProvider::wrap` reading the validator with
//! `.read()` written over three lines.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use proc_macro2::{Delimiter, Ident, TokenStream, TokenTree};
use syn::visit::{self, Visit};

/// Every `.rs` file under `dir`, parsed. A file `syn` cannot parse fails the scan, rather
/// than being passed over.
fn parse_sources(dir: &Path, out: &mut Vec<(PathBuf, syn::File)>) {
    let entries = fs::read_dir(dir).expect("the crate's src directory must be readable");
    for entry in entries {
        let path = entry.expect("a readable directory entry").path();
        if path.is_dir() {
            parse_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let text = fs::read_to_string(&path).expect("a readable .rs file");
            let file = syn::parse_file(&text)
                .unwrap_or_else(|e| panic!("{} does not parse: {e}", path.display()));
            out.push((path, file));
        }
    }
}

/// The statics a static_init `dynamic` attribute with `drop` declares: the ones finalized at
/// process exit. Found in the code rather than listed, so that a new one is covered.
#[derive(Default)]
struct Finalizable(BTreeSet<String>);

impl<'ast> Visit<'ast> for Finalizable {
    fn visit_item_static(&mut self, item: &'ast syn::ItemStatic) {
        let drops = item.attrs.iter().any(|attr| {
            let dynamic = attr
                .path()
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "dynamic");
            let drop = match &attr.meta {
                syn::Meta::List(list) => list
                    .tokens
                    .clone()
                    .into_iter()
                    .any(|token| matches!(token, TokenTree::Ident(ident) if ident == "drop")),
                _ => false,
            };
            dynamic && drop
        });
        if drops {
            self.0.insert(item.ident.to_string());
        }
        visit::visit_item_static(self, item);
    }
}

/// A read of one of `statics` with `.read()`, and every other use of one, in a file.
struct Reads<'a> {
    statics: &'a BTreeSet<String>,
    /// The line and the static of each `.read()`.
    panicking: Vec<(usize, String)>,
    /// The statics this file uses, by any name it parses: a method call, a path, a macro's
    /// argument.
    used: BTreeSet<String>,
}

impl<'a> Reads<'a> {
    fn new(statics: &'a BTreeSet<String>) -> Self {
        Self {
            statics,
            panicking: Vec::new(),
            used: BTreeSet::new(),
        }
    }

    fn is_static(&self, ident: &Ident) -> Option<String> {
        let name = ident.to_string();
        self.statics.contains(&name).then_some(name)
    }

    /// The static an expression names: a path, possibly in parentheses.
    fn static_named(&self, expr: &syn::Expr) -> Option<String> {
        match expr {
            syn::Expr::Paren(inner) => self.static_named(&inner.expr),
            syn::Expr::Group(inner) => self.static_named(&inner.expr),
            syn::Expr::Path(path) => self.is_static(&path.path.segments.last()?.ident),
            _ => None,
        }
    }

    /// A macro's arguments, which `syn` leaves as tokens: `STATIC . read ( )`, however it is
    /// laid out.
    fn scan_tokens(&mut self, tokens: TokenStream) {
        let tokens: Vec<TokenTree> = tokens.into_iter().collect();
        for (i, token) in tokens.iter().enumerate() {
            match token {
                TokenTree::Group(group) => self.scan_tokens(group.stream()),
                TokenTree::Ident(ident) => {
                    let Some(name) = self.is_static(ident) else {
                        continue;
                    };
                    self.used.insert(name.clone());
                    if let (
                        Some(TokenTree::Punct(dot)),
                        Some(TokenTree::Ident(read)),
                        Some(TokenTree::Group(args)),
                    ) = (tokens.get(i + 1), tokens.get(i + 2), tokens.get(i + 3))
                    {
                        if dot.as_char() == '.'
                            && read == "read"
                            && args.delimiter() == Delimiter::Parenthesis
                            && args.stream().is_empty()
                        {
                            self.panicking.push((read.span().start().line, name));
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for Reads<'_> {
    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if call.method == "read" && call.args.is_empty() {
            if let Some(name) = self.static_named(&call.receiver) {
                self.panicking.push((call.method.span().start().line, name));
            }
        }
        visit::visit_expr_method_call(self, call);
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.scan_tokens(mac.tokens.clone());
        visit::visit_macro(self, mac);
    }

    /// Everything but the name: a static's declaration is not a use of it.
    fn visit_item_static(&mut self, item: &'ast syn::ItemStatic) {
        for attr in &item.attrs {
            self.visit_attribute(attr);
        }
        self.visit_type(&item.ty);
        self.visit_expr(&item.expr);
    }

    fn visit_ident(&mut self, ident: &'ast Ident) {
        if let Some(name) = self.is_static(ident) {
            self.used.insert(name);
        }
    }
}

/// The `.read()`s of `statics` in `source`, as (line, static).
fn panicking_reads(source: &str, statics: &BTreeSet<String>) -> Vec<(usize, String)> {
    let file = syn::parse_file(source).expect("the sample parses");
    let mut reads = Reads::new(statics);
    reads.visit_file(&file);
    reads.panicking
}

/// No production site may use the PANICKING accessor on a finalizable static.
///
/// Two-sided: the scan must also find the statics, and see each of them used, or a rename
/// or a construct it does not parse would make it vacuous and it would pass by looking at
/// nothing.
#[test]
fn no_production_site_reads_a_finalizable_static_with_the_panicking_accessor() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    parse_sources(&src, &mut files);
    assert!(
        !files.is_empty(),
        "no .rs files found under {src:?} -- fix the walk"
    );

    let mut finalizable = Finalizable::default();
    for (_, file) in &files {
        finalizable.visit_file(file);
    }
    let statics = finalizable.0;
    assert!(
        !statics.is_empty(),
        "no `#[dynamic(.., drop)]` static found under {src:?} -- fix the attribute match"
    );

    let mut used = BTreeSet::new();
    let mut offenders = Vec::new();
    for (path, file) in &files {
        let mut reads = Reads::new(&statics);
        reads.visit_file(file);
        used.extend(reads.used);
        for (line, name) in reads.panicking {
            offenders.push(format!("{}:{line} -- {name}.read()", path.display()));
        }
    }

    // NON-VACUITY: a static the scan never sees used is one it cannot check.
    let unseen: Vec<_> = statics.difference(&used).collect();
    assert!(
        unseen.is_empty(),
        "the scan sees no use of {unseen:?} beside its declaration, so it checks nothing \
         for it -- fix the scan"
    );

    assert!(
        offenders.is_empty(),
        "these sites would PANIC if the static is already FINALIZED at process \
         exit -- use `try_live_read!` and fail, as `alloc_resources` does, or \
         `.try_read()` where the static is already initialized, as `Segment::drop` \
         does:\n  {}",
        offenders.join("\n  ")
    );
}

/// THE CONTROL for the scan above: it sees the panicking accessor however the call is laid
/// out, and nothing else.
#[test]
fn the_scan_tells_the_panicking_accessor_from_the_others() {
    let statics = BTreeSet::from(["GLOBAL_VALIDATOR".to_string()]);
    for (source, line) in [
        // `ShmProvider::wrap` until BER-2486: three lines.
        (
            "fn f() {\n    GLOBAL_VALIDATOR\n        .read()\n        .add(x);\n}",
            3,
        ),
        ("fn f() { GLOBAL_VALIDATOR.read().add(x); }", 1),
        (
            "fn f() { crate::watchdog::validator::GLOBAL_VALIDATOR.read(); }",
            1,
        ),
        ("fn f() { (GLOBAL_VALIDATOR).read(); }", 1),
        // In a macro's arguments, which are tokens to `syn`.
        (
            "fn f() {\n    debug!(\"{:?}\", GLOBAL_VALIDATOR\n        .read());\n}",
            3,
        ),
    ] {
        assert_eq!(
            panicking_reads(source, &statics),
            vec![(line, "GLOBAL_VALIDATOR".to_string())],
            "must be flagged, on line {line}: {source}"
        );
    }

    // The accepted forms, a mention, prose, and another receiver.
    for source in [
        "fn f() { if let Ok(v) = GLOBAL_VALIDATOR.try_read() { v.add(x); } }",
        "fn f() { GLOBAL_VALIDATOR.init(); }",
        "fn f() { let v = try_live_read!(GLOBAL_VALIDATOR); }",
        "fn f() { GLOBAL_VALIDATOR.read_only(); }",
        "fn f() { GLOBAL_VALIDATOR.read(timeout); }",
        "/// `GLOBAL_VALIDATOR.read()` panics at exit.\nfn f() {}",
        "fn f() {\n    // GLOBAL_VALIDATOR.read()\n}",
        "fn f() { let s = \"GLOBAL_VALIDATOR.read()\"; }",
        "fn f() { OTHER.read(); }",
        "macro_rules! m { ($st:path) => { $st.read() }; }",
    ] {
        assert_eq!(
            panicking_reads(source, &statics),
            vec![],
            "must not be flagged: {source}"
        );
    }
}

/// THE CONTROL for the attribute match: a static is finalizable when its `dynamic` attribute
/// says `drop`, however the attribute is written.
#[test]
fn the_scan_finds_the_statics_finalized_at_exit() {
    let file = syn::parse_file(
        "#[dynamic(lazy, drop)] static mut A: u8 = 0;\n\
         #[static_init::dynamic(drop)] static B: u8 = 0;\n\
         #[dynamic(lazy)] static mut C: u8 = 0;\n\
         #[other(drop)] static D: u8 = 0;\n\
         static E: u8 = 0;\n\
         mod m { #[dynamic(lazy, drop)] pub(crate) static mut F: u8 = 0; }",
    )
    .unwrap();
    let mut finalizable = Finalizable::default();
    finalizable.visit_file(&file);
    assert_eq!(
        finalizable.0,
        BTreeSet::from(["A".to_string(), "B".to_string(), "F".to_string()])
    );
}
