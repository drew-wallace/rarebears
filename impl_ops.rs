use std::ops::Add;
use std::ops::Sub;
use std::ops::Mul;
use std::ops::Div;
use std::ops::Rem;
use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::BitXor;
use std::ops::Shl;
use std::ops::Shr;
use std::ops::Index;

use std::fmt;

#[derive(Debug)]
pub enum Columns {
    SeriesBool(Series<bool>),
    SeriesI8(Series<i8>),
    SeriesU8(Series<u8>),
    SeriesI16(Series<i16>),
    SeriesU16(Series<u16>),
    SeriesI32(Series<i32>),
    SeriesU32(Series<u32>),
    SeriesI64(Series<i64>),
    SeriesU64(Series<u64>),
    SeriesI128(Series<i128>),
    SeriesU128(Series<u128>),
    SeriesF32(Series<f32>),
    SeriesF64(Series<f64>),
}

impl Columns {
    fn to_variant_string(&self) -> String {
        match self {
            Columns::SeriesBool(_) => format!("Columns::SeriesBool"),
            Columns::SeriesI8(_) => format!("Columns::SeriesI8"),
            Columns::SeriesU8(_) => format!("Columns::SeriesU8"),
            Columns::SeriesI16(_) => format!("Columns::SeriesI16"),
            Columns::SeriesU16(_) => format!("Columns::SeriesU16"),
            Columns::SeriesI32(_) => format!("Columns::SeriesI32"),
            Columns::SeriesU32(_) => format!("Columns::SeriesU32"),
            Columns::SeriesI64(_) => format!("Columns::SeriesI64"),
            Columns::SeriesU64(_) => format!("Columns::SeriesU64"),
            Columns::SeriesI128(_) => format!("Columns::SeriesI128"),
            Columns::SeriesU128(_) => format!("Columns::SeriesU128"),
            Columns::SeriesF32(_) => format!("Columns::SeriesF32"),
            Columns::SeriesF64(_) => format!("Columns::SeriesF64"),
        }
    }
}

impl fmt::Display for Columns {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Columns::SeriesBool(series) => write!(f, "{}", series),
            Columns::SeriesI8(series) => write!(f, "{}", series),
            Columns::SeriesU8(series) => write!(f, "{}", series),
            Columns::SeriesI16(series) => write!(f, "{}", series),
            Columns::SeriesU16(series) => write!(f, "{}", series),
            Columns::SeriesI32(series) => write!(f, "{}", series),
            Columns::SeriesU32(series) => write!(f, "{}", series),
            Columns::SeriesI64(series) => write!(f, "{}", series),
            Columns::SeriesU64(series) => write!(f, "{}", series),
            Columns::SeriesI128(series) => write!(f, "{}", series),
            Columns::SeriesU128(series) => write!(f, "{}", series),
            Columns::SeriesF32(series) => write!(f, "{}", series),
            Columns::SeriesF64(series) => write!(f, "{}", series),
        }
    }
}

macro_rules! series {
    ($x:expr, $i:path) => {
        {
            match $x {
                $i(q) => q,
                _ => panic!("Wrong type associated with Series"),
            }
        }
    };
}

pub struct Dataframe {
    pub collection: Vec<Columns>
}

impl fmt::Display for Dataframe {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let collection: Vec<String> = self.collection.iter().map(|column| format!("{}", column)).collect();
        write!(f, "[{}]", collection.join(", "))
    }
}

impl Index<usize> for Dataframe {
    type Output = Columns;

    fn index(&self, i: usize) -> &Columns {
        &self.collection[i]
    }
}

#[derive(Debug)]
pub struct Series<T> {
    pub rows: Vec<T>
}

impl<T> Index<usize> for Series<T> {
    type Output = T;

    fn index(&self, i: usize) -> &T {
        &self.rows[i]
    }
}

impl<T: std::fmt::Debug> fmt::Display for Series<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.rows)
    }
}

macro_rules! impl_op(($type:ident, $trait:ident, $operator:tt, $trait_func:ident) => (
    // Series + Series
    impl $trait<Series<$type>> for Series<$type> {
        type Output = Series<$type>;

        fn $trait_func(self, _rhs: Series<$type>) -> Series<$type> {
            let new_rows: Vec<_> = self.rows.iter().zip(_rhs.rows).map(|(a, b)| a $operator b).collect();
            Series{rows: new_rows}
        }
    }

    // Series + &Series
    impl $trait<&Series<$type>> for Series<$type> {
        type Output = Series<$type>;

        fn $trait_func(self, _rhs: &Series<$type>) -> Series<$type> {
            let new_rows: Vec<_> = self.rows.iter().zip(&_rhs.rows).map(|(a, b)| a $operator b).collect();
            Series{rows: new_rows}
        }
    }

    // &Series + Series
    impl $trait<Series<$type>> for &Series<$type> {
        type Output = Series<$type>;

        fn $trait_func(self, _rhs: Series<$type>) -> Series<$type> {
            let new_rows: Vec<_> = self.rows.iter().zip(_rhs.rows).map(|(a, b)| a $operator b).collect();
            Series{rows: new_rows}
        }
    }

    // &Series + &Series
    impl $trait<&Series<$type>> for &Series<$type> {
        type Output = Series<$type>;

        fn $trait_func(self, _rhs: &Series<$type>) -> Series<$type> {
            let new_rows: Vec<_> = self.rows.iter().zip(&_rhs.rows).map(|(a, b)| a $operator b).collect();
            Series{rows: new_rows}
        }
    }

    // Series + scalar
    impl $trait<$type> for Series<$type> {
        type Output = Series<$type>;

        fn $trait_func(self, _rhs: $type) -> Series<$type> {
            let new_rows: Vec<_> = self.rows.iter().map(|a| a $operator _rhs).collect();
            Series{rows: new_rows}
        }
    }

    // &Series + scalar
    impl $trait<$type> for &Series<$type> {
        type Output = Series<$type>;

        fn $trait_func(self, _rhs: $type) -> Series<$type> {
            let new_rows: Vec<_> = self.rows.iter().map(|a| a $operator _rhs).collect();
            Series{rows: new_rows}
        }
    }

    // scalar + Series
    impl $trait<Series<$type>> for $type {
        type Output = Series<$type>;

        fn $trait_func(self, _rhs: Series<$type>) -> Series<$type> {
            let new_rows: Vec<_> = _rhs.rows.iter().map(|a| self $operator a).collect();
            Series{rows: new_rows}
        }
    }

    // scalar + &Series
    impl $trait<&Series<$type>> for $type {
        type Output = Series<$type>;

        fn $trait_func(self, _rhs: &Series<$type>) -> Series<$type> {
            let new_rows: Vec<_> = _rhs.rows.iter().map(|a| self $operator a).collect();
            Series{rows: new_rows}
        }
    }
));

macro_rules! impl_num_ops(($type:ident) => (
    impl_op!($type, Add, +, add);
    impl_op!($type, Sub, -, sub);
    impl_op!($type, Mul, *, mul);
    impl_op!($type, Div, /, div);
    impl_op!($type, Rem, %, rem);
));

macro_rules! impl_bit_ops(($type:ident) => (
    impl_op!($type, Shl, <<, shl);
    impl_op!($type, Shr, >>, shr);
));

macro_rules! impl_logic_ops(($type:ident) => (
    impl_op!($type, BitAnd, &, bitand);
    impl_op!($type, BitOr, |, bitor);
    impl_op!($type, BitXor, ^, bitxor);
));

macro_rules! impl_all_ops(($type:ident) => (
    impl_num_ops!($type);
    impl_bit_ops!($type);
    impl_logic_ops!($type);
));

impl_logic_ops!(bool);
impl_all_ops!(i8);
impl_all_ops!(u8);
impl_all_ops!(i16);
impl_all_ops!(u16);
impl_all_ops!(i32);
impl_all_ops!(u32);
impl_all_ops!(i64);
impl_all_ops!(u64);
impl_all_ops!(i128);
impl_all_ops!(u128);
impl_num_ops!(f32);
impl_num_ops!(f64);

macro_rules! impl_op_col(($trait:ident, $operator:tt, $operator_string:tt, $trait_func:ident, $($variant:path),+) => (
    // &Columms + &Columms
    impl $trait<&Columns> for &Columns {
        type Output = Columns;

        fn $trait_func(self, _rhs: &Columns) -> Columns {
            $(
                if let $variant(lhs) = self {
                    let new_lhs = Series{rows: lhs.rows.to_vec()};
                    if let $variant(rhs) = _rhs {
                        let new_rhs = Series{rows: rhs.rows.to_vec()};
                        return $variant(new_lhs $operator new_rhs);
                    } else {
                        panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, _rhs.to_variant_string(), self.to_variant_string(),);
                    }
                }
            )+

            panic!("error: cannot {} `{}` not implemented", $operator_string, self.to_variant_string());
        }
    }

    // &Columms + Columms
    impl $trait<Columns> for &Columns {
        type Output = Columns;

        fn $trait_func(self, _rhs: Columns) -> Columns {
            $(
                if let $variant(lhs) = self {
                    let new_lhs = Series{rows: lhs.rows.to_vec()};
                    if let $variant(rhs) = _rhs {
                        let new_rhs = Series{rows: rhs.rows.to_vec()};
                        return $variant(new_lhs $operator new_rhs);
                    } else {
                        panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, _rhs.to_variant_string(), self.to_variant_string(),);
                    }
                }
            )+

            panic!("error: cannot {} `{}` not implemented", $operator_string, self.to_variant_string());
        }
    }

    // &Columms + Columms
    impl $trait<&Columns> for Columns {
        type Output = Columns;

        fn $trait_func(self, _rhs: &Columns) -> Columns {
            $(
                if let $variant(lhs) = &self {
                    let new_lhs = Series{rows: lhs.rows.to_vec()};
                    if let $variant(rhs) = _rhs {
                        let new_rhs = Series{rows: rhs.rows.to_vec()};
                        return $variant(new_lhs $operator new_rhs);
                    } else {
                        panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, _rhs.to_variant_string(), self.to_variant_string(),);
                    }
                }
            )+

            panic!("error: cannot {} `{}` not implemented", $operator_string, self.to_variant_string());
        }
    }
));

macro_rules! impl_num_ops_col (($($variant:path),+) => (
    impl_op_col!(Add, +, "add", add, $($variant),+);
    impl_op_col!(Sub, -, "sub", sub, $($variant),+);
    impl_op_col!(Mul, *, "mul", mul, $($variant),+);
    impl_op_col!(Div, /, "div", div, $($variant),+);
    impl_op_col!(Rem, %, "rem", rem, $($variant),+);
));

macro_rules! impl_bit_ops_col(($($variant:path),+) => (
    impl_op_col!(Shl, <<, "shl", shl, $($variant),+);
    impl_op_col!(Shr, >>, "shr", shr, $($variant),+);
));

macro_rules! impl_logic_ops_col(($($variant:path),+) => (
    impl_op_col!(BitAnd, &, "bitand", bitand, $($variant),+);
    impl_op_col!(BitOr, |, "bitor", bitor, $($variant),+);
    impl_op_col!(BitXor, ^, "bitxor", bitxor, $($variant),+);
));

impl_num_ops_col!(Columns::SeriesF32, Columns::SeriesF64, Columns::SeriesI8, Columns::SeriesU8, Columns::SeriesI16, Columns::SeriesU16, Columns::SeriesI32, Columns::SeriesU32, Columns::SeriesI64, Columns::SeriesU64, Columns::SeriesI128, Columns::SeriesU128);
impl_bit_ops_col!(Columns::SeriesI8, Columns::SeriesU8, Columns::SeriesI16, Columns::SeriesU16, Columns::SeriesI32, Columns::SeriesU32, Columns::SeriesI64, Columns::SeriesU64, Columns::SeriesI128, Columns::SeriesU128);
impl_logic_ops_col!(Columns::SeriesBool, Columns::SeriesI8, Columns::SeriesU8, Columns::SeriesI16, Columns::SeriesU16, Columns::SeriesI32, Columns::SeriesU32, Columns::SeriesI64, Columns::SeriesU64, Columns::SeriesI128, Columns::SeriesU128);

macro_rules! impl_op_col_scalar(($type:ident, $trait:ident, $operator:tt, $operator_string:expr, $trait_func:ident, $variant:path) => (
    // Columns + Series
    impl $trait<Series<$type>> for Columns {
        type Output = Columns;

        fn $trait_func(self, _rhs: Series<$type>) -> Columns {
            if let $variant(lhs) = self {
                let new_lhs = Series{rows: lhs.rows.to_vec()};
                return $variant(new_lhs $operator _rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, std::any::type_name::<$type>(), self.to_variant_string());
            }
        }
    }

    // Columns + &Series
    impl $trait<&Series<$type>> for Columns {
        type Output = Columns;

        fn $trait_func(self, _rhs: &Series<$type>) -> Columns {
            if let $variant(lhs) = self {
                let new_lhs = Series{rows: lhs.rows.to_vec()};
                return $variant(new_lhs $operator _rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, std::any::type_name::<$type>(), self.to_variant_string());
            }
        }
    }

    // &Columns + Series
    impl $trait<Series<$type>> for &Columns {
        type Output = Columns;

        fn $trait_func(self, _rhs: Series<$type>) -> Columns {
            if let $variant(lhs) = self {
                let new_lhs = Series{rows: lhs.rows.to_vec()};
                return $variant(new_lhs $operator _rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, std::any::type_name::<$type>(), self.to_variant_string());
            }
        }
    }

    // &Columns + &Series
    impl $trait<&Series<$type>> for &Columns {
        type Output = Columns;

        fn $trait_func(self, _rhs: &Series<$type>) -> Columns {
            if let $variant(lhs) = self {
                let new_lhs = Series{rows: lhs.rows.to_vec()};
                return $variant(new_lhs $operator _rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, std::any::type_name::<$type>(), self.to_variant_string());
            }
        }
    }

    // Series + Columns
    impl $trait<Columns> for Series<$type> {
        type Output = Columns;

        fn $trait_func(self, _rhs: Columns) -> Columns {
            if let $variant(rhs) = _rhs {
                let new_rhs = Series{rows: rhs.rows.to_vec()};
                return $variant(self $operator new_rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, _rhs.to_variant_string(), std::any::type_name::<$type>());
            }
        }
    }

    // Series + &Columns
    impl $trait<&Columns> for Series<$type> {
        type Output = Columns;

        fn $trait_func(self, _rhs: &Columns) -> Columns {
            if let $variant(rhs) = _rhs {
                let new_rhs = Series{rows: rhs.rows.to_vec()};
                return $variant(self $operator new_rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, _rhs.to_variant_string(), std::any::type_name::<$type>());
            }
        }
    }

    // &Series + Columns
    impl $trait<Columns> for &Series<$type> {
        type Output = Columns;

        fn $trait_func(self, _rhs: Columns) -> Columns {
            if let $variant(rhs) = _rhs {
                let new_rhs = Series{rows: rhs.rows.to_vec()};
                return $variant(self $operator new_rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, _rhs.to_variant_string(), std::any::type_name::<$type>());
            }
        }
    }

    // &Series + &Columns
    impl $trait<&Columns> for &Series<$type> {
        type Output = Columns;

        fn $trait_func(self, _rhs: &Columns) -> Columns {
            if let $variant(rhs) = _rhs {
                let new_rhs = Series{rows: rhs.rows.to_vec()};
                return $variant(self $operator new_rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, _rhs.to_variant_string(), std::any::type_name::<$type>());
            }
        }
    }

    // Columns + scalar
    impl $trait<$type> for Columns {
        type Output = Columns;

        fn $trait_func(self, _rhs: $type) -> Columns {
            if let $variant(lhs) = self {
                let new_lhs = Series{rows: lhs.rows.to_vec()};
                return $variant(new_lhs $operator _rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, std::any::type_name::<$type>(), self.to_variant_string());
            }
        }
    }

    // scalar + Columns
    impl $trait<Columns> for $type {
        type Output = Columns;

        fn $trait_func(self, _rhs: Columns) -> Columns {
            if let $variant(rhs) = _rhs {
                let new_rhs = Series{rows: rhs.rows.to_vec()};
                return $variant(self $operator new_rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, _rhs.to_variant_string(), std::any::type_name::<$type>());
            }
        }
    }

    // &Columns + scalar
    impl $trait<$type> for &Columns {
        type Output = Columns;

        fn $trait_func(self, _rhs: $type) -> Columns {
            if let $variant(lhs) = self {
                let new_lhs = Series{rows: lhs.rows.to_vec()};
                return $variant(new_lhs $operator _rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, std::any::type_name::<$type>(), self.to_variant_string());
            }
        }
    }

    // scalar + &Columns
    impl $trait<&Columns> for $type {
        type Output = Columns;

        fn $trait_func(self, _rhs: &Columns) -> Columns {
            if let $variant(rhs) = _rhs {
                let new_rhs = Series{rows: rhs.rows.to_vec()};
                return $variant(self $operator new_rhs);
            } else {
                panic!("error: cannot {} `{}` to `{}` mismatched types", $operator_string, _rhs.to_variant_string(), std::any::type_name::<$type>());
            }
        }
    }
));

macro_rules! impl_num_ops_col_scalar(($type:ident, $variant:path) => (
    impl_op_col_scalar!($type, Add, +, "add", add, $variant);
    impl_op_col_scalar!($type, Sub, -, "sub", sub, $variant);
    impl_op_col_scalar!($type, Mul, *, "mul", mul, $variant);
    impl_op_col_scalar!($type, Div, /, "div", div, $variant);
    impl_op_col_scalar!($type, Rem, %, "rem", rem, $variant);
));

macro_rules! impl_bit_ops_col_scalar(($type:ident, $variant:path) => (
    impl_op_col_scalar!($type, Shl, <<, "shl", shl, $variant);
    impl_op_col_scalar!($type, Shr, >>, "shr", shr, $variant);
));

macro_rules! impl_logic_ops_col_scalar(($type:ident, $variant:path) => (
    impl_op_col_scalar!($type, BitAnd, &, "bitand", bitand, $variant);
    impl_op_col_scalar!($type, BitOr, |, "bitor", bitor, $variant);
    impl_op_col_scalar!($type, BitXor, ^, "bitxor", bitxor, $variant);
));

macro_rules! impl_all_ops_col_scalar(($type:ident, $variant:path) => (
    impl_num_ops_col_scalar!($type, $variant);
    impl_bit_ops_col_scalar!($type, $variant);
    impl_logic_ops_col_scalar!($type, $variant);
));

impl_logic_ops_col_scalar!(bool, Columns::SeriesBool);
impl_all_ops_col_scalar!(i8, Columns::SeriesI8);
impl_all_ops_col_scalar!(u8, Columns::SeriesU8);
impl_all_ops_col_scalar!(i16, Columns::SeriesI16);
impl_all_ops_col_scalar!(u16, Columns::SeriesU16);
impl_all_ops_col_scalar!(i32, Columns::SeriesI32);
impl_all_ops_col_scalar!(u32, Columns::SeriesU32);
impl_all_ops_col_scalar!(i64, Columns::SeriesI64);
impl_all_ops_col_scalar!(u64, Columns::SeriesU64);
impl_all_ops_col_scalar!(i128, Columns::SeriesI128);
impl_all_ops_col_scalar!(u128, Columns::SeriesU128);
impl_num_ops_col_scalar!(f32, Columns::SeriesF32);
impl_num_ops_col_scalar!(f64, Columns::SeriesF64);
