#[macro_use(series)]
extern crate rarebears;

use rarebears::Dataframe;
use rarebears::Columns;
use rarebears::Series;

fn main() {
	let a = Series{rows: vec![1, 2, 3]};
    let b = Series{rows: vec![2, 3, 4]};
    println!("a = {}", a);
    println!("a[0] = {}", a[0]);
	println!("a + 7 = {}", &a + 7);
    println!("7 + b = {}", 7 + &b);
    println!("a + b = {}", &a + &b);
    println!("a + b + b = {}", &a + &b + &b);
    println!("a + 7 + b = {}", &a + 7 + &b);

    let a = Series{rows: vec![1, 2, 3]};
    let b = Series{rows: vec![2, 3, 4]};
    let c = Series{rows: vec![true, true, false, false]};
    let d = Series{rows: vec![true, false, true, false]};
    let df = Dataframe{collection: vec![Columns::SeriesI8(a), Columns::SeriesI8(b), Columns::SeriesBool(c), Columns::SeriesBool(d)]};
    println!("df = {}", df);
    println!("df[0] = {}", &df[0]);
    println!("df[0][0] = {}", series!(&df[0], Columns::SeriesI8)[0]);
    println!("df[0] + 7 = {}", &df[0] + 7i8);
    println!("7 + df[1] = {}", 7i8 + &df[1]);
    println!("df[0] + df[1] = {}", &df[0] + &df[1]);
    println!("df[0] + df[1] + df[1] = {}", &df[0] + &df[1] + &df[1]);
    println!("df[0] + 7 + df[1] = {}", &df[0] + 7i8 + &df[1]);
    println!("df[2] & df[3] = {}", &df[2] & &df[3]);
    println!("df[2] | df[3] = {}", &df[2] | &df[3]);

    let a = Series{rows: vec![1i8, 2i8, 3i8]};
    let b = Series{rows: vec![2i8, 3i8, 4i8]};
    println!("df[0] + b = {}", &df[0] + b);
    println!("a + df[1] = {}", a + &df[1]);

    let a = Series{rows: vec![1i8, 2i8, 3i8]};
    let b = Series{rows: vec![2i8, 3i8, 4i8]};
    println!("df[0] + b = {}", &df[0] + &b);
    println!("a + df[1] = {}", &a + &df[1]);
    println!("a + df[1] + b = {}", &a + &df[1] + &b);
    println!("a + df[1] + 7 + b + 7 = {}", &a + &df[1] + 7i8 + &b + 7i8);
}