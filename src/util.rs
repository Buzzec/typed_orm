use std::borrow::Cow;

pub fn format_column_name<'a>(
    parent_name: Option<&str>,
    name: &'a str,
    name_separator: &str,
) -> Cow<'a, str> {
    match parent_name {
        None => name.into(),
        Some(parent_name) => format!("{}{}{}", parent_name, name_separator, name).into(),
    }
}

pub trait ReferenceStruct {
    type Owned;

    fn clone_to_owned(&self) -> Self::Owned;
}

#[derive(Debug, Default)]
pub struct RowIdxAccumulator(usize);
impl RowIdxAccumulator {
    pub fn next_idx(&mut self) -> usize {
        let idx = self.0;
        self.0 += 1;
        idx
    }
}
