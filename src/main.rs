use clap::{clap_app, value_t};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;

const REPLICATION: u16 = 3;

struct Matrix {
    rows: u16,
    cols: u16,
    data: Vec<Vec<u16>>,
}

impl Matrix {
    pub fn new(r: u16, c: u16) -> Matrix {
        Matrix {
            rows: r,
            cols: c,
            data: vec![vec![0; r as usize]; c as usize],
        }
    }
}

impl fmt::Display for Matrix {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let data: &Vec<Vec<u16>> = &self.data;
        writeln!(f, "** Shard Distribution ::").unwrap();
        for c in 0..self.cols {
            write!(f, "N-{:^03} ", c)?;
        }
        writeln!(f, "")?;
        for r in 0..self.rows {
            for c in 0..self.cols {
                let col = &data[c as usize];
                write!(f, "[{:<3}] ", col[r as usize])?;
            }
            writeln!(f, "")?;
        }
        write!(f, "")
    }
}

pub trait DotGraph {
    fn g(&self, fail_col: Option<u16>, output_file: &str) -> ();
}

impl DotGraph for Matrix {
    fn g(&self, fail_col: Option<u16>, output_file: &str) -> () {
        let fail_col_values = fail_col.and_then(|x| {
            let mut set = HashSet::new();
            for r in 0..self.rows {
                set.insert(self.data[x as usize][r as usize]);
            }
            Some(set)
        });

        let mut f = File::create(output_file).unwrap();
        writeln!(f, "digraph G {{").unwrap();
        writeln!(f, "\trankdir=LR;").unwrap();
        let data = &self.data;
        for c in 0..self.cols {
            writeln!(
                f,
                "\thost{} [shape=none label=<<table><tr><td bgcolor=\"red\">HOST-{}</td></tr>",
                c, c,
            )
            .unwrap();
            let mut hit_cnt = 0;
            for r in 0..self.rows {
                let v = data[c as usize][r as usize];
                let group = r / REPLICATION;
                let top = r % REPLICATION == 0;
                match fail_col_values
                    .as_ref()
                    .and_then(|x| if x.contains(&v) { Some(v) } else { None })
                {
                    Some(_) => {
                        hit_cnt += 1;
                        if top {
                            writeln!(
                                f,
                                "<tr><td bgcolor=\"blue\" port=\"g{}{}\">{}</td></tr>",
                                c, group, v
                            )
                            .unwrap();
                        } else {
                            writeln!(f, "<tr><td bgcolor=\"blue\">{}</td></tr>", v).unwrap();
                        }
                    }
                    None => {
                        if top {
                            writeln!(f, "<tr><td port=\"g{}{}\">{}</td></tr>", c, group, v)
                                .unwrap();
                        } else {
                            writeln!(f, "<tr><td>{}</td></tr>", v).unwrap();
                        }
                    }
                }
                writeln!(f, "<tr><td bgcolor=\"green\">{}</td></tr>", hit_cnt).unwrap();
                writeln!(f, "</table>>];").unwrap();
            }
        }
        let color_cycle = ["black", "red"];
        for group in 0..self.rows / REPLICATION {
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
    }
}

fn main() {
    let matches = clap_app!(app =>
                            (version: "1.0")
                            (author: "ChinaXing")
                            (about: "calculate shard distribution on multiple nodes")
                            (@arg matrix: -m --matrix "show matrix")
                            (@arg deltaByRankOrder: -d --delta "show fail distribution delta change by rank-order")
                            (@arg rankOrder: -r --rankOrder +takes_value "rank cycle size")
                            (@arg failColumn: -f --failColumn +takes_value "mark failed column, start from 0")
                            (@arg generateGraph: -g --generateDotGraph +takes_value "generate dot graph to file")
                            (@arg shardsPerNode: +required "shard num per node")
                            (@arg nodeCount: +required "Node count")
            )
    .get_matches();
    let rows = value_t!(matches.value_of("shardsPerNode"), u16).unwrap();
    let cols = value_t!(matches.value_of("nodeCount"), u16).unwrap();
    let fail_col = value_t!(matches.value_of("failColumn"), u16);
    let rank_order = value_t!(matches.value_of("rankOrder"), u16).unwrap();
    let graph = matches.is_present("generateGraph");
    let delta = matches.is_present("deltaByRankOrder");
    let m = distribute(rows, cols, rank_order);
    if matches.is_present("matrix") {
        println!("{}", m);
    }
    let fc: Option<u16> = fail_col.ok().clone();
    if graph {
        let graph_file = matches.value_of("generateGraph").unwrap();
        m.g(fc, graph_file);
    }

    if delta && fc.is_some() {
        println!("** Fail Delta Distribution ::");
        let fail_dist = fail_distri_with_rank_order(rows, cols, fc.unwrap());
        let mut keys = fail_dist.keys().collect::<Vec<&u16>>();
        keys.sort();
        for k in keys {
            let vs = fail_dist.get(k).unwrap();
            let delta = vs.iter().max().unwrap() - vs.iter().min().unwrap();
            println!("{}: delta: {} => {:?}", k, delta, fail_dist.get(k).unwrap());
        }
    }
}

fn distribute(rows: u16, cols: u16, rank_order: u16) -> Matrix {
    let mut m: Matrix = Matrix::new(rows, cols);
    for r in 0..rows {
        let replication_index = r % REPLICATION;
        let rank = (r / REPLICATION) % rank_order + 1;
        let incremental = if replication_index == 0 {
            0
        } else {
            (rank * replication_index) + (replication_index - 1)
        };
        let base = (r / REPLICATION) * cols;
        for c in 0..cols {
            let v = base + ((incremental + c) % cols);
            let cl = &mut m.data[c as usize];
            cl[r as usize] = v;
        }
    }
    m
}

fn fail_distri_with_rank_order(rows: u16, cols: u16, fail_col: u16) -> HashMap<u16, Vec<u16>> {
    let mut result = HashMap::new();
    for rank in 1..cols {
        let m = distribute(rows, cols, rank);
        result.insert(rank, calc_fail_distri(&m, fail_col));
    }
    result
}

fn calc_fail_distri(m: &Matrix, fail_col: u16) -> Vec<u16> {
    let mut result = vec![];
    let col = &m.data[fail_col as usize];
    let mut col_nodes = HashSet::new();
    for r in 0..m.rows {
        col_nodes.insert(col[r as usize]);
    }
    for c in 0..m.cols {
        if c == fail_col {
            continue;
        }
        let mut cnt = 0;
        let mut fails: Vec<String> = vec![];
        for r in 0..m.rows {
            let cell = m.data[c as usize][r as usize];
            if col_nodes.contains(&cell) {
                fails.push(format!("({},{}){}", c, r, cell));
                cnt += 1;
            }
        }
        result.push(cnt as u16);
    }
    result
}
