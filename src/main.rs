use clap::{clap_app, value_t};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;

const REPLICATION: usize = 3;

struct Matrix {
    rows: usize,
    cols: usize,
    replication: usize,
    rank_order: usize,
    data: Vec<Vec<usize>>,
}

impl Matrix {
    pub fn new(r: usize, c: usize, rep: usize, order: usize) -> Matrix {
        Matrix {
            rows: r,
            cols: c,
            replication: rep,
            rank_order: order,
            data: vec![vec![0; r as usize]; c as usize],
        }
    }
}

impl fmt::Display for Matrix {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let data: &Vec<Vec<usize>> = &self.data;
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
    fn g(&self, fail_col: Option<usize>, output_file: &str) -> ();
}

impl DotGraph for Matrix {
    fn g(&self, fail_col: Option<usize>, output_file: &str) -> () {
        let mut leader_rows = BTreeSet::new();
        for r in (0..self.rows).step_by(self.replication) {
            let rank = r / self.replication / self.rank_order;
            let leader_row = if rank % 2 == 0 { r + 1 } else { r };
            leader_rows.insert(leader_row);
        }
        println!("Leader_rows : {:?}", leader_rows);

        let mut fail_leaders = fail_col.and_then(|x| {
            let col_data = &self.data[x];
            let result: Vec<usize> = leader_rows.iter().map(|c| col_data[*c]).collect();
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
        let mut leader_failover: Vec<(usize, usize)> = vec![];
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
                                        z[group as usize] = 9999 as usize;
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
                            (@arg deltaByRankOrder: -d --delta "show fail distribution delta change by rank-order")
                            (@arg rankOrder: -r --rankOrder +takes_value "rank cycle size")
                            (@arg failColumn: -f --failColumn +takes_value "mark failed column, start from 0")
                            (@arg generateGraph: -g --generateDotGraph +takes_value "generate dot graph to file")
                            (@arg shardsPerNode: +required "shard num per node")
                            (@arg nodeCount: +required "Node count")
            )
    .get_matches();
    let rows = value_t!(matches.value_of("shardsPerNode"), usize).unwrap();
    let cols = value_t!(matches.value_of("nodeCount"), usize).unwrap();
    let fail_col = value_t!(matches.value_of("failColumn"), usize);
    let rank_order = value_t!(matches.value_of("rankOrder"), usize).unwrap();
    let graph = matches.is_present("generateGraph");
    let delta = matches.is_present("deltaByRankOrder");
    let m = distribute(rows, cols, rank_order);
    if matches.is_present("matrix") {
        println!("{}", m);
    }
    let fc: Option<usize> = fail_col.ok().clone();
    if graph {
        let graph_file = matches.value_of("generateGraph").unwrap();
        m.g(fc, graph_file);
    }

    if delta && fc.is_some() {
        println!("** Fail Delta Distribution ::");
        let fail_dist = fail_distri_with_rank_order(rows, cols, fc.unwrap());
        let mut keys = fail_dist.keys().collect::<Vec<&usize>>();
        keys.sort();
        for k in keys {
            let vs = fail_dist.get(k).unwrap();
            let delta = vs.iter().max().unwrap() - vs.iter().min().unwrap();
            println!("{}: delta: {} => {:?}", k, delta, fail_dist.get(k).unwrap());
        }
    }
}

fn distribute(rows: usize, cols: usize, rank_order: usize) -> Matrix {
    let mut m: Matrix = Matrix::new(rows, cols, REPLICATION, rank_order);
    for r in 0..rows {
        let replication_index = r % m.replication;
        let rank = (r / m.replication) % rank_order + 1;
        let incremental = if replication_index == 0 {
            0
        } else {
            (rank * replication_index) + (replication_index - 1)
        };
        let base = (r / m.replication) * cols;
        let put_order_asc = rank % 2 == 1;
        for c in 0..cols {
            let v = base + ((incremental + c) % cols);
            let idx = if put_order_asc {
                c as usize
            } else {
                (cols - 1 - c) as usize
            };
            let cl = &mut m.data[idx];
            cl[r as usize] = v;
        }
    }
    m
}

fn fail_distri_with_rank_order(
    rows: usize,
    cols: usize,
    fail_col: usize,
) -> HashMap<usize, Vec<usize>> {
    let mut result = HashMap::new();
    for rank in 1..cols {
        let m = distribute(rows, cols, rank);
        result.insert(rank, calc_fail_distri(&m, fail_col));
    }
    result
}

fn calc_fail_distri(m: &Matrix, fail_col: usize) -> Vec<usize> {
    let mut result = vec![];
    let col = &m.data[fail_col];
    let mut col_nodes = BTreeSet::new();
    for r in 0..m.rows {
        col_nodes.insert(col[r]);
    }
    for c in 0..m.cols {
        if c == fail_col {
            continue;
        }
        let mut cnt = 0;
        let mut fails: Vec<String> = vec![];
        for r in 0..m.rows {
            let cell = m.data[c][r];
            if col_nodes.contains(&cell) {
                fails.push(format!("({},{}){}", c, r, cell));
                cnt += 1;
            }
        }
        result.push(cnt);
    }
    result
}
