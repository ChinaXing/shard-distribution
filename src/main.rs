use clap::{clap_app, value_t};
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::prelude::*;

#[macro_use]
extern crate lazy_static;

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

lazy_static! {
    static ref S18: Vec<usize> = [ // skip 8 (18/2-1)
        (1usize..=7).collect::<Vec<usize>>().as_slice(),
        (9..=17).collect::<Vec<usize>>().as_slice(),
        &[2usize],
    ]
    .concat();
    static ref S42: Vec<usize> = [ // skip 20 (42/2-1)
        (1usize..=19).collect::<Vec<usize>>().as_slice(),
        (21..=41).collect::<Vec<usize>>().as_slice(),
        &[2usize],
    ]
    .concat();
}

pub fn build_third_shift(rank_cycle: usize, rank_size: usize) -> Vec<usize> {
    let r = if rank_size % 2 == 1 {
        (1usize..=rank_cycle).collect::<Vec<usize>>()
    } else {
        match (rank_cycle, rank_size) {
            (17, 18) => S18.to_vec(),
            (41, 42) => S42.to_vec(),
            _ => panic!("can not handle rank_cycle !!"),
        }
    };
    println!("- Third Replica Shift: {:?}", r);
    r
}

impl Matrix {
    pub fn new(r: usize, c: usize, rep: usize, s: i32, third_failover: bool) -> Matrix {
        let rc = if (c - 1) % 2 == 0 {
            if third_failover {
                // if third replica can failover as leader ?
                (c - 1) / 2
            } else {
                c - 1
            }
        } else {
            c - 1
        };
        println!("- Rank Cycle : {}", rc);
        let ts = build_third_shift(rc, c);
        // check cols
        if c % (rep * rc) != 0 {
            panic!(
                "cols must multiply of : replication * rank_cycle = {} * {}",
                rep, rc
            );
        }
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

macro_rules! write_file_option {
    ($file:expr, $($arg:tt)*) => (
        if $file.is_some() {
            let mut f = $file.unwrap();
            writeln!(f, $($arg)*).unwrap();
        }
    );
}

pub trait DotGraph {
    fn g(&self, fail_col: Option<usize>, output_file: Option<&str>) -> ();
}

impl DotGraph for Matrix {
    fn g(&self, fail_col: Option<usize>, output_file: Option<&str>) -> () {
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
        let mut node_leader_failover_to_nr: Vec<usize> = vec![0; self.cols];
        let mut node_failover_to_nr: Vec<usize> = vec![0; self.cols];
        let fr = output_file.and_then(|x| {
            let f = File::create(x).unwrap();
            Some(f)
        });
        let file = fr.as_ref();
        write_file_option!(file, "digraph G {{");
        write_file_option!(file, "\trankdir=LR;");
        let data = &self.data;
        for c in 0..self.cols {
            write_file_option!(
                file,
                "\thost{} [shape=none label=<<table><tr><td bgcolor=\"black\"><font color=\"white\">Node-{}</font></td></tr>",
                c, c,
            );
            let mut fail_hit_cnt = 0;
            let mut leader_hit_cnt = 0;
            for r in 0..self.rows {
                let v = data[c as usize][r as usize];
                let group = r / self.replication;
                let failover_leader_target_replica =
                    (group / self.rank_cycle) % (self.cols / self.rank_cycle) + 1;
                //println!("the fo_l_t_r : {}", failover_leader_target_replica);
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
                                    if z[group as usize] == v
                                        && r % self.replication == failover_leader_target_replica
                                    {
                                        z[group as usize] = -v as i64;
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
                            write_file_option!(
                                file,
                                "<tr><td bgcolor=\"{}\" port=\"g{}{}\">{}</td></tr>",
                                color,
                                c,
                                group,
                                v
                            );
                        } else {
                            write_file_option!(
                                file,
                                "<tr><td bgcolor=\"{}\">{}</td></tr>",
                                color,
                                v
                            );
                        }
                    }
                    None => {
                        let color = if is_leader_row { "orange" } else { "white" };
                        if top {
                            write_file_option!(
                                file,
                                "<tr><td bgcolor=\"{}\" port=\"g{}{}\">{}</td></tr>",
                                color,
                                c,
                                group,
                                v
                            );
                        } else {
                            write_file_option!(
                                file,
                                "<tr><td bgcolor=\"{}\">{}</td></tr>",
                                color,
                                v
                            );
                        }
                    }
                }
            }
            write_file_option!(file, "<tr><td bgcolor=\"green\">{}</td></tr>", fail_hit_cnt);
            write_file_option!(
                file,
                "<tr><td bgcolor=\"yellow\">{}</td></tr>",
                leader_hit_cnt
            );
            write_file_option!(file, "</table>>];");
            node_failover_to_nr[c] = fail_hit_cnt;
            node_leader_failover_to_nr[c] = leader_hit_cnt;
        }
        let color_cycle = ["black", "red"];
        for group in 0..self.rows / self.replication {
            let color = color_cycle[(group % 2) as usize];
            for c in 0..self.cols - 1 {
                if c > 0 {
                    write_file_option!(
                        file,
                        "\thost{}:g{}{} -> host{}:g{}{} [ color=\"{}\" ];",
                        c,
                        c,
                        group,
                        c + 1,
                        c + 1,
                        group,
                        color
                    );
                } else {
                    write_file_option!(
                        file,
                        "\thost{}:g{}{} -> host{}:g{}{} [ label=\"group{}\" color=\"{}\"];",
                        c,
                        c,
                        group,
                        c + 1,
                        c + 1,
                        group,
                        group,
                        color
                    );
                }
            }
        }
        write_file_option!(file, "}}");
        if file.is_some() {
            file.unwrap().flush().unwrap();
        }
        println!("\nLeader_fail_over:\n {:?}", leader_failover);
        println!("\nFailover distribution:");
        for c in 0..self.cols {
            println!(
                "{:>w$} -> fail_over_hit: {} leader_hit: {}",
                c,
                node_failover_to_nr[c],
                node_leader_failover_to_nr[c],
                w = 2
            );
        }
    }
}

fn main() {
    let matches = clap_app!(app =>
                            (version: "1.0")
                            (author: "ChinaXing")
                            (about: "calculate shard distribution on multiple nodes")
                            (@arg matrix: -m --matrix "show matrix")
                            (@arg thirdReplicaFailoverable: -t --thirdReplicaFailoverable "third replica can takeover for leader")
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
    let start_index = value_t!(matches.value_of("matrixStart"), i32);
    let trf = matches.is_present("thirdReplicaFailoverable");
    let m = distribute(rows, cols, start_index.unwrap_or(0), trf);
    if matches.is_present("matrix") {
        println!("{}", m);
    }
    if matches.is_present("javaArray") {
        println!("** Java Array ::");
        m.to_java();
    }
    let fc: Option<usize> = fail_col.ok();
    let graph_file: Option<&str> = matches.value_of("generateGraph");
    m.g(fc, graph_file);
}

fn distribute(rows: usize, cols: usize, start: i32, third_replica_failoverable: bool) -> Matrix {
    let mut m: Matrix = Matrix::new(rows, cols, REPLICATION, start, third_replica_failoverable);
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
