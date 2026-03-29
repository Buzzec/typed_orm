pub fn generic_suffix<T>() -> String {
    let type_name = std::any::type_name::<T>();
    let Some(start) = type_name.find('<') else {
        return String::new();
    };
    let Some(end) = type_name.rfind('>') else {
        return String::new();
    };

    sanitize(&type_name[start + 1..end])
}

fn sanitize(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut prev_was_sep = false;

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_was_sep = false;
        } else if !prev_was_sep {
            out.push('_');
            prev_was_sep = true;
        }
    }

    while out.ends_with('_') {
        out.pop();
    }

    out
}
