use clap::{clap_app, value_t};
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;

const REPLICATION: usize = 3;

struct Matrix {
    start_from: i32,
    rows: usize,
    cols: usize,
    replication: usize,
    rank_cycle: usize,
    third_shift: Vec<usize>,
    data: Vec<Vec<i64>>,
}

pub fn build_third_shift(rank_cycle: usize, rank_size: usize) -> Vec<usize> {
    match (rank_cycle, rank_size) {
        (17, 18) => vec![1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 2],
        _ => panic!("can not handle rank_cycle !!"),
    }
}

impl Matrix {
    pub fn new(r: usize, c: usize, rep: usize, s: i32) -> Matrix {
        let rc = if (c - 1) % 2 == 0 { (c - 1) / 2 } else { c - 1 };
        let ts = build_third_shift(rc, c);
        Matrix {
            start_from: s,
            rows: r,
            cols: c,
            replication: rep,
            rank_cycle: rc,
            third_shift: ts,
            data: vec![vec![0; r as usize]; c as usize],
        }
    }
}

pub trait DumpToJava {
    fn to_java(&self) -> ();
}

impl DumpToJava for Matrix {
    fn to_java(&self) -> () {
        let data = &self.data;
        println!("new int[][]{{");
        for c in 0..self.cols {
            if c != 0 {
                println!(",");
            }
            print!("\tnew int[]{{");
            for r in 0..self.rows {
                if r == 0 {
                    print!("{}", data[c][r]);
                } else {
                    print!(", {}", data[c][r]);
                }
            }
            print!("}}");
        }
        println!("\n}}");
    }
}

impl fmt::Display for Matrix {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let data: &Vec<Vec<i64>> = &self.data;
        let col_width: usize = (self.cols * self.rows).to_string().len();

        writeln!(f, "** Shard Distribution ::").unwrap();
        write!(f, "{:<w$}", ' ', w = col_width + 1)?;
        for c in 0..self.cols {
            write!(f, "N-{c:<0w$} ", c = c, w = col_width)?;
        }
        writeln!(f, "")?;
        let mut distinc_each_col: Vec<HashSet<i64>> = vec![];
        for _ in 0..self.cols {
            distinc_each_col.push(HashSet::new());
        }
        for r in 0..self.rows {
            write!(f, "{:<w$} ", r + 1, w = col_width)?;
            for c in 0..self.cols {
                let col = &data[c as usize];
                let v = col[r as usize];
                if r % self.replication < self.replication - 1 {
                    write!(f, "[{:>w$}] ", v, w = col_width)?;
                } else {
                    write!(f, "<{:>w$}> ", v, w = col_width)?;
                }
                distinc_each_col[c].insert(v);
            }
            writeln!(f, "")?;
        }
        write!(f, "    ")?;
        for c in 0..self.cols {
            write!(f, " {:>w$}  ", distinc_each_col[c].len(), w = col_width)?;
        }
        write!(f, "\n")
    }
}

pub trait DotGraph {
    fn g(&self, fail_col: Option<usize>, output_file: &str) -> ();
}

impl DotGraph for Matrix {
    fn g(&self, fail_col: Option<usize>, output_file: &str) -> () {
        let mut leader_rows = BTreeSet::new();
        for r in (0..self.rows).step_by(self.replication) {
            leader_rows.insert(r);
        }
        println!("Leader_rows : {:?}", leader_rows);

        let mut fail_leaders = fail_col.and_then(|x| {
            let col_data = &self.data[x];
            let result: Vec<i64> = leader_rows.iter().map(|c| col_data[*c]).collect();
            Some(result)
        });
        println!("Failed_leaders : {:?}", fail_leaders);

        let fail_col_values = fail_col.and_then(|x| {
            let mut set = BTreeSet::new();
            for r in 0..self.rows {
                set.insert(self.data[x as usize][r as usize]);
            }
            Some(set)
        });

        // shard-number -> node-number(column-no)
        let mut leader_failover: Vec<(i64, usize)> = vec![];
        let mut f = File::create(output_file).unwrap();
        writeln!(f, "digraph G {{").unwrap();
        writeln!(f, "\trankdir=LR;").unwrap();
        let data = &self.data;
        for c in 0..self.cols {
            writeln!(
                f,
                "\thost{} [shape=none label=<<table><tr><td bgcolor=\"black\"><font color=\"white\">Node-{}</font></td></tr>",
                c, c,
            )
            .unwrap();
            let mut fail_hit_cnt = 0;
            let mut leader_hit_cnt = 0;
            for r in 0..self.rows {
                let v = data[c as usize][r as usize];
                let group = r / self.replication;
                let top = r % self.replication == 0;
                let is_leader_row = leader_rows.contains(&r);
                let fail_cell =
                    fail_col_values
                        .as_ref()
                        .and_then(|x| if x.contains(&v) { Some(v) } else { None });
                match fail_cell {
                    Some(_) => {
                        fail_hit_cnt += 1;
                        let is_failover_leader = c != fail_col.unwrap()
                            && fail_leaders
                                .as_mut()
                                .and_then(|z| {
                                    if z[group as usize] == v && r % self.replication <= 1 {
                                        z[group as usize] = 9999 as i64;
                                        leader_failover.push((v, c));
                                        Some(&v)
                                    } else {
                                        None
                                    }
                                })
                                .is_some();
                        let color = if is_failover_leader {
                            leader_hit_cnt += 1;
                            "red"
                        } else {
                            "blue"
                        };
                        if top {
                            writeln!(
                                f,
                                "<tr><td bgcolor=\"{}\" port=\"g{}{}\">{}</td></tr>",
                                color, c, group, v
                            )
                            .unwrap();
                        } else {
                            writeln!(f, "<tr><td bgcolor=\"{}\">{}</td></tr>", color, v).unwrap();
                        }
                    }
                    None => {
                        let color = if is_leader_row { "orange" } else { "white" };
                        if top {
                            writeln!(
                                f,
                                "<tr><td bgcolor=\"{}\" port=\"g{}{}\">{}</td></tr>",
                                color, c, group, v
                            )
                            .unwrap();
                        } else {
                            writeln!(f, "<tr><td bgcolor=\"{}\">{}</td></tr>", color, v).unwrap();
                        }
                    }
                }
            }
            writeln!(f, "<tr><td bgcolor=\"green\">{}</td></tr>", fail_hit_cnt).unwrap();
            writeln!(f, "<tr><td bgcolor=\"yellow\">{}</td></tr>", leader_hit_cnt).unwrap();
            writeln!(f, "</table>>];").unwrap();
        }
        let color_cycle = ["black", "red"];
        for group in 0..self.rows / self.replication {
            let color = color_cycle[(group % 2) as usize];
            for c in 0..self.cols - 1 {
                if c > 0 {
                    writeln!(
                        f,
                        "\thost{}:g{}{} -> host{}:g{}{} [ color=\"{}\" ];",
                        c,
                        c,
                        group,
                        c + 1,
                        c + 1,
                        group,
                        color
                    )
                    .unwrap();
                } else {
                    write!(
                        f,
                        "\thost{}:g{}{} -> host{}:g{}{} [ label=\"group{}\" color=\"{}\"];",
                        c,
                        c,
                        group,
                        c + 1,
                        c + 1,
                        group,
                        group,
                        color
                    )
                    .unwrap();
                }
            }
        }
        writeln!(f, "}}").unwrap();
        f.flush().unwrap();
        println!("\nleader_fail_over:\n {:?}", leader_failover);
    }
}

fn main() {
    let matches = clap_app!(app =>
                            (version: "1.0")
                            (author: "ChinaXing")
                            (about: "calculate shard distribution on multiple nodes")
                            (@arg matrix: -m --matrix "show matrix")
                            (@arg failColumn: -f --failColumn +takes_value "mark failed column, start from 0")
                            (@arg generateGraph: -g --generateDotGraph +takes_value "generate dot graph to file")
                            (@arg javaArray: -j --generateJavaArray "generate a javaArray for matrix")
                            (@arg matrixStart: -s --matrixStart +takes_value "start no of matrix, default 0")
                            (@arg shardsPerNode: +required "shard num per node")
                            (@arg nodeCount: +required "Node count")
            )
    .get_matches();
    let rows = value_t!(matches.value_of("shardsPerNode"), usize).unwrap();
    let cols = value_t!(matches.value_of("nodeCount"), usize).unwrap();
    let fail_col = value_t!(matches.value_of("failColumn"), usize);
    let graph = matches.is_present("generateGraph");
    let start_index = value_t!(matches.value_of("matrixStart"), i32);
    let m = distribute(rows, cols, start_index.unwrap_or(0));
    if matches.is_present("matrix") {
        println!("{}", m);
    }
    if matches.is_present("javaArray") {
        println!("** Java Array ::");
        m.to_java();
    }
    let fc: Option<usize> = fail_col.ok().clone();
    if graph {
        let graph_file = matches.value_of("generateGraph").unwrap();
        m.g(fc, graph_file);
    }
}

fn distribute(rows: usize, cols: usize, start: i32) -> Matrix {
    let mut m: Matrix = Matrix::new(rows, cols, REPLICATION, start);
    for r in 0..rows {
        let rank = r / m.replication;
        let rep_index = r % m.replication;
        let base = rank * cols;
        let offset = match rep_index {
            0 => 0,
            1 => rank % m.rank_cycle + 1,
            2 => rank % m.rank_cycle + 1 + m.third_shift[rank % m.rank_cycle],
            _ => panic!("invalid rep_index"),
        };
        for c in 0..cols {
            let v = base + (offset + c) % cols;
            let cl = &mut m.data[c];
            cl[r as usize] = (v as i64) + (m.start_from as i64);
        }
    }
    m
}
