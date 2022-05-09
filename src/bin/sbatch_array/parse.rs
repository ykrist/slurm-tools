use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ArgumentToken {
    Arg(String),
    Sep { files: bool, zip: bool },
    Index(usize),
}

impl From<ArgumentToken> for String {
    fn from(t: ArgumentToken) -> Self {
        use ArgumentToken::*;
        match t {
            Arg(s) => s,
            Index(i) => format!("{{{}}}", i),
            Sep { files, zip } => {
                let mut s = ":::".to_string();
                if files {
                    s.push(':')
                }
                if zip {
                    s.push('+')
                }
                s
            }
        }
    }
}

impl ArgumentToken {
    fn is_sep(&self) -> bool {
        matches!(self, ArgumentToken::Sep { .. })
    }
}

impl From<&str> for ArgumentToken {
    fn from(s: &str) -> Self {
        use ArgumentToken::*;
        match s {
            ":::" => Sep {
                files: false,
                zip: false,
            },
            ":::+" => Sep {
                files: false,
                zip: true,
            },
            "::::" => Sep {
                files: true,
                zip: false,
            },
            "::::+" => Sep {
                files: true,
                zip: true,
            },
            s => {
                if let Some(s) = s.strip_prefix('{') {
                    if let Some(s) = s.strip_suffix('}') {
                        if let Ok(i) = s.parse() {
                            return Index(i);
                        }
                    }
                }
                Arg(s.to_string())
            }
        }
    }
}

fn get_template(command: &mut Vec<ArgumentToken>) -> Result<Vec<ArgumentToken>> {
    let mut template_end = command.len();
    let mut n_args = 0;
    for (k, c) in command.iter().enumerate().rev() {
        if c.is_sep() {
            template_end = k;
            n_args += 1;
        }
    }
    for c in &command[..template_end] {
        if let &ArgumentToken::Index(i) = c {
            if i >= n_args {
                bail!(
                    "Index given is {} but only {} argument lists given",
                    i,
                    n_args
                );
            }
        }
    }
    let mut tmpl = command.split_off(template_end);
    std::mem::swap(&mut tmpl, command);
    for i in (0..n_args).map(ArgumentToken::Index) {
        if !tmpl.contains(&i) {
            tmpl.push(i)
        }
    }
    Ok(tmpl)
}

pub fn parse_command(
    mut command: Vec<ArgumentToken>,
) -> Result<(Vec<ArgumentToken>, Option<ArgumentList>)> {
    let template = get_template(&mut command)?;
    if command.is_empty() {
        return Ok((template, None));
    }

    let mut zip_with_prev = vec![];
    let mut arglists = vec![];

    let starts = command
        .iter()
        .enumerate()
        .filter_map(|(k, s)| if s.is_sep() { Some(k) } else { None });
    let ends = starts.clone().skip(1).chain([command.len()]);

    for (i, j) in starts.zip(ends) {
        let mut arglist = vec![];
        let (files, zip) = match &command[i] {
            &ArgumentToken::Sep { files, zip } => (files, zip),
            _ => unreachable!(),
        };
        if zip_with_prev.is_empty() && zip {
            bail!("first argument list cannot be zipped with previous")
        }

        for t in &command[(i + 1)..j] {
            let t = t.clone();
            if files {
                read_lines_from_file(String::from(t), &mut arglist)?;
            } else {
                arglist.push(t.clone().into());
            }
        }
        arglists.push(arglist);
        zip_with_prev.push(zip);
    }
    let arg_iter = ArgumentList::new(&zip_with_prev, arglists);
    Ok((template, Some(arg_iter)))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ZipGroup {
    start: usize,
    end: usize,
    members: usize,
}

#[derive(Clone, Debug)]
pub struct ArgumentList {
    groups: Vec<ZipGroup>,
    arglists: Vec<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct ArgumentListIter<'a> {
    index_buf: Vec<usize>,
    l: &'a ArgumentList,
}

impl ArgumentList {
    fn new(zip_with_previous: &[bool], mut arglists: Vec<Vec<String>>) -> Self {
        assert_eq!(zip_with_previous.len(), arglists.len());
        assert!(!arglists.is_empty());
        let mut groups = vec![];
        let mut start = 0;
        let mut members = arglists[0].len();
        debug_assert_eq!(zip_with_previous[0], false);
        for (end, (z, args)) in zip_with_previous.iter().zip(&arglists).enumerate().skip(1) {
            if !z {
                groups.push(ZipGroup {
                    start,
                    end,
                    members,
                });
                start = end;
                members = args.len();
            } else {
                members = members.min(args.len());
            };
        }
        groups.push(ZipGroup {
            start,
            end: arglists.len(),
            members,
        });
        for g in &groups {
            for k in g.start..g.end {
                arglists[k].truncate(g.members);
            }
        }
        ArgumentList { groups, arglists }
    }

    pub fn iter(&self) -> ArgumentListIter {
        let index_buf = vec![0; self.groups.len()];
        ArgumentListIter { index_buf, l: self }
    }

    pub fn indices(&self, i: usize) -> Result<Vec<usize>> {
        let array_inds: Result<Vec<usize>> = {
            let args = self.arglists.get(i).ok_or_else(|| {
                anyhow!(
                    "Argument with index {} reference but only {} argument lists given",
                    i,
                    self.arglists.len()
                )
            })?;
            args.iter()
                .map(|s| {
                    s.parse()
                        .map_err(|_| anyhow!("Index argument list must be non-negative integers."))
                })
                .collect()
        };
        let mut array_inds = array_inds?;
        array_inds.sort();

        let inner_period = self.groups[(i + 1)..]
            .iter()
            .fold(1, |acc, x| acc * x.members);
        let outer_period = self.groups[..i].iter().fold(1, |acc, x| acc * x.members);
        let n = array_inds.len() * inner_period * outer_period;

        let mut inds = Vec::with_capacity(n);
        for _ in 0..outer_period {
            for &k in &array_inds {
                for _ in 0..inner_period {
                    inds.push(k)
                }
            }
        }
        Ok(inds)
    }
}

impl<'a> ArgumentListIter<'a> {
    fn inc_indices(&mut self) {
        for k in (0..self.index_buf.len()).rev() {
            self.index_buf[k] += 1;
            if self.index_buf[k] >= self.l.groups[k].members && k > 0 {
                self.index_buf[k] = 0;
            } else {
                return;
            }
        }
    }
}
impl<'a> Iterator for ArgumentListIter<'a> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index_buf.is_empty() {
            return None;
        }
        if self.index_buf[0] >= self.l.groups[0].members {
            return None;
        }

        let mut output = Vec::with_capacity(self.l.arglists.len());

        for (&k, group) in self.index_buf.iter().zip(&self.l.groups) {
            for i in group.start..group.end {
                output.push(self.l.arglists[i][k].clone())
            }
        }

        debug_assert_eq!(output.len(), self.l.arglists.len());
        self.inc_indices();
        Some(output)
    }
}

pub fn substitute_args(templ: &[ArgumentToken], arglist: Option<ArgumentList>) -> Vec<Vec<String>> {
    let arglist = match arglist {
        Some(a) => a,
        None => return vec![templ.iter().cloned().map(String::from).collect()],
    };

    arglist
        .iter()
        .map(|args| {
            let mut v = Vec::with_capacity(templ.len());
            for t in templ {
                match t {
                    ArgumentToken::Sep { .. } => unreachable!(),
                    ArgumentToken::Arg(s) => v.push(s.clone()),
                    ArgumentToken::Index(i) => v.push(args[*i].clone()),
                }
            }
            v
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tokens() {
        assert_eq!(ArgumentToken::from("{1}"), ArgumentToken::Index(1));
        assert_eq!(
            ArgumentToken::from("{1"),
            ArgumentToken::Arg("{1".to_string())
        );
        assert_eq!(
            ArgumentToken::from(":::"),
            ArgumentToken::Sep {
                files: false,
                zip: false
            }
        );
        assert_eq!(
            ArgumentToken::from(":::+"),
            ArgumentToken::Sep {
                files: false,
                zip: true
            }
        );
        assert_eq!(
            ArgumentToken::from("::::"),
            ArgumentToken::Sep {
                files: true,
                zip: false
            }
        );
        assert_eq!(
            ArgumentToken::from("::::+"),
            ArgumentToken::Sep {
                files: true,
                zip: true
            }
        );
    }

    macro_rules! svec {
        ($($t:tt)*) => {
            [$($t)*].into_iter().map(String::from).collect::<Vec<_>>()
        };
    }

    #[test]
    fn arg_iter_single() {
        let i = ArgumentList::new(&[false], vec![svec!["a", "b", "c"]]);
        assert_eq!(
            &i.groups,
            &vec![ZipGroup {
                start: 0,
                end: 1,
                members: 3
            }]
        );
        let v: Vec<_> = i.iter().collect();
        assert_eq!(v, vec![svec!["a"], svec!["b"], svec!["c"]]);
    }

    #[test]
    fn arg_iter_multigroup() {
        let i = ArgumentList::new(
            &[false, true, true, false],
            vec![
                svec!["a", "b", "c"],
                svec!["x", "y", "z"],
                svec!["1", "2", "3", "4"],
                svec!["i", "j"],
            ],
        );
        assert_eq!(
            &i.groups,
            &vec![
                ZipGroup {
                    start: 0,
                    end: 3,
                    members: 3
                },
                ZipGroup {
                    start: 3,
                    end: 4,
                    members: 2
                },
            ]
        );
        let v: Vec<_> = i.iter().collect();
        assert_eq!(
            v,
            vec![
                svec!["a", "x", "1", "i"],
                svec!["a", "x", "1", "j"],
                svec!["b", "y", "2", "i"],
                svec!["b", "y", "2", "j"],
                svec!["c", "z", "3", "i"],
                svec!["c", "z", "3", "j"],
            ]
        );
    }

    #[test]
    #[should_panic]
    fn arg_iter_empty() {
        ArgumentList::new(&[], vec![]);
    }

    fn expand_args(s: &str) -> Result<Vec<Vec<String>>> {
        let (template, args) = parse_command(s.split_whitespace().map(From::from).collect())?;
        Ok(substitute_args(&template, args))
    }

    #[test]
    fn generate_args() -> Result<()> {
        assert_eq!(expand_args("echo ::: 1")?, vec![svec!["echo", "1"]]);
        assert_eq!(
            expand_args("echo ::: 1 ::: 2 ::: 3")?,
            vec![svec!["echo", "1", "2", "3"]]
        );
        assert_eq!(
            expand_args("echo ::: 1 2 :::+ 3")?,
            vec![svec!["echo", "1", "3"]]
        );
        assert_eq!(
            expand_args("echo ::: 1 2 ::: 3 :::+ 4")?,
            vec![svec!["echo", "1", "3", "4"], svec!["echo", "2", "3", "4"],]
        );
        assert_eq!(
            expand_args("echo ::: 1 2 ::: 3 :::+ 4")?,
            vec![svec!["echo", "1", "3", "4"], svec!["echo", "2", "3", "4"],]
        );

        assert!(expand_args("echo :::+ 1").is_err());
        Ok(())
    }
}
