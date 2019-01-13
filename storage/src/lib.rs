extern crate byteorder;

use std::mem;
use std::rc::Rc;

mod db_value;

#[cfg(target_pointer_width = "64")]
const POINTER_SIZE: usize = 8;
#[cfg(target_pointer_width = "32")]
const POINTER_SIZE: usize = 4;

#[derive(Debug)]
struct Table {
    name: String,
    schema: Rc<Schema>,
    fixed_data: Vec<u8>,
    variable_data: Vec<u8>,
}

impl Table {

    fn new<S>(name: S, schema: Rc<Schema>) -> Self where S: Into<String> {
        Table {
            name: name.into(),
            schema,
            fixed_data: Vec::new(),
            variable_data: Vec::new(),
        }
    }

    fn row_length(&self) -> usize {
        self.schema.iter().fold(0, |acc, field_spec| acc + field_spec.size())
    }
}

#[derive(Debug)]
struct FieldSpec {
    name: String,
    type_spec: TypeSpec,
}

type Schema = Vec<FieldSpec>;

impl FieldSpec {
    fn new<S>(name: S, type_spec: TypeSpec) -> Self where S: Into<String>  {
        FieldSpec {
            name: name.into(),
            type_spec,
        }
    }

    fn size(&self) -> usize {
        self.type_spec.size()
    }
}

#[derive(Debug)]
struct TypeSpec {
    db_type: DbType,
    is_nullable: bool,
    default: Option<Vec<u8>>,
}

impl TypeSpec {
    fn new(db_type: DbType, is_nullable: bool, default: Option<Vec<u8>>) -> Self {
        TypeSpec {
            db_type,
            is_nullable,
            default,
        }
    }

    fn size(&self) -> usize {
        self.db_type.size()
    }
}

// Idea: Rename to InternalDBType and create a DbType trait that defines
// (initially) read/write methods
#[derive(Debug)]
enum DbType {
    Boolean,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Varchar(usize),
    Blob,
}

impl DbType {
    fn size(&self) -> usize {
        match *self {
            DbType::Boolean => 1,
            DbType::Int32 => 4,
            DbType::UInt32 => 4,
            DbType::Int64 => 8,
            DbType::UInt64 => 8,
            DbType::Varchar(len) if len < 256 => 1 + len,
            DbType::Varchar(len)              => 2 + POINTER_SIZE,
            DbType::Blob => 2 + POINTER_SIZE,
        }
    }
}

fn read_value<T: Clone>(buf: &Vec<u8>, offset: usize) -> T {
    let size = mem::size_of::<T>();
    let src = &buf[offset..(offset+size)];
    let src_ptr: *const u8 = src.as_ptr();
    let out_ptr: *const T = src_ptr as *const _;
    let out: &T = unsafe { &*out_ptr };
    out.clone()
}

fn write_value<T: Clone>(buf: &mut Vec<u8>, offset: usize, val: T) {
    let size = mem::size_of::<T>();
    let src_ptr = &val as *const T as *const u8;
    let dest = &mut buf[offset..(offset+size)];
    let dest_ptr: *mut u8 = dest.as_mut_ptr();
    unsafe {
        std::ptr::copy_nonoverlapping(src_ptr, dest_ptr, size);
    };
}

trait DbValue {
    fn size(&self) -> usize;
    fn read_from_buffer(&mut self, buf: &[u8]) -> Result<(), String>;
    fn write_to_buffer(&self, buf: &mut [u8]) -> Result<(), String>;
}

#[derive(Debug, PartialEq, Eq)]
struct DBUInt64(u64);

impl DBUInt64 {
    fn new() -> Self {
        DBUInt64(0)
    }
}

impl DbValue for DBUInt64 {
    fn size(&self) -> usize {
        8
    }

    #[cfg(target_endian = "little")]
    fn read_from_buffer(&mut self, buf: &[u8]) -> Result<(), String> {
        if let [b0, b1, b2, b3, b4, b5, b6, b7] = buf {
            self.0 =
            (*b0 as u64)
            | ((*b1 as u64) << 8)
            | ((*b2 as u64) << 16)
            | ((*b3 as u64) << 24)
            | ((*b4 as u64) << 32)
            | ((*b5 as u64) << 40)
            | ((*b6 as u64) << 48)
            | ((*b7 as u64) << 56);

            Ok(())
        } else {
            Err(format!("Invalid buffer length: {}", buf.len()))
        }
    }

    #[cfg(target_endian = "little")]
    fn write_to_buffer(&self, buf: &mut [u8]) -> Result<(), String> {
        if let [b0, b1, b2, b3, b4, b5, b6, b7] = buf {
            *b0 = (self.0 & 0xff) as u8;
            *b1 = ((self.0 >> 8) & 0xff) as u8;
            *b2 = ((self.0 >> 16) & 0xff) as u8;
            *b3 = ((self.0 >> 24) & 0xff) as u8;
            *b4 = ((self.0 >> 32) & 0xff) as u8;
            *b5 = ((self.0 >> 40) & 0xff) as u8;
            *b6 = ((self.0 >> 48) & 0xff) as u8;
            *b7 = ((self.0 >> 56) & 0xff) as u8;
            Ok(())
        } else {
            Err(format!("Invalid buffer length: {}", buf.len()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_data_from_buffer() {
        let bytes: Vec<u8> = vec![0, 0];
        let val = read_value::<u16>(&bytes, 0);
        assert_eq!(0, val);

        let bytes: Vec<u8> = vec![4, 1];
        let val = read_value::<u16>(&bytes, 0);
        assert_eq!(260, val);
    }

    #[test]
    fn write_data_to_buffer() {
        let mut bytes: Vec<u8> = vec![0, 0];
        write_value::<u16>(&mut bytes, 0, 0);
        assert_eq!(vec![0, 0], bytes);

        let mut bytes: Vec<u8> = vec![0, 0];
        write_value::<u16>(&mut bytes, 0, 260);
        assert_eq!(vec![4, 1], bytes);
    }

    #[test]
    fn data_buffer_roundtrip() {
        let mut bytes = vec![0; 16];
        write_value(&mut bytes, 0, 5280u64);
        write_value(&mut bytes, 9, 12345u32);
        write_value(&mut bytes, 13, 512u16);
        write_value(&mut bytes, 15, 128u8);

        assert_eq!(5280, read_value::<u64>(&bytes, 0));
        assert_eq!(12345, read_value::<u32>(&bytes, 9));
        assert_eq!(512, read_value::<u16>(&bytes, 13));
        assert_eq!(128, read_value::<u8>(&bytes, 15));
    }

    #[test]
    fn fixed_row_length() {
        let schema1 = Rc::new(vec![
            FieldSpec::new("id", TypeSpec::new(DbType::UInt64, false, None)),
        ]);
        let table1 = Table::new("test 1", schema1.clone());
        assert_eq!(8, table1.row_length());

        let schema2 = Rc::new(vec![
            FieldSpec::new("id", TypeSpec::new(DbType::UInt64, false, None)),
            FieldSpec::new("age", TypeSpec::new(DbType::UInt32, false, None)),
            FieldSpec::new("is_active", TypeSpec::new(DbType::Boolean, false, None)),
            FieldSpec::new("notes", TypeSpec::new(DbType::Varchar(1000), false, None)),
            FieldSpec::new("image", TypeSpec::new(DbType::Blob, false, None)),
        ]);
        let table2 = Table::new("test 2", schema2.clone());
        assert_eq!(17 + 2*POINTER_SIZE, table2.row_length());
    }

    #[test]
    fn variable_row_length() {
        let table1 = Table::new("test 1", Rc::new(vec![
            FieldSpec::new("name", TypeSpec::new(DbType::Varchar(30), false, None)),
        ]));
        assert_eq!(31, table1.row_length());

        let table2 = Table::new("test 1", Rc::new(vec![
            FieldSpec::new("title", TypeSpec::new(DbType::Varchar(30), false, None)),
            FieldSpec::new("description", TypeSpec::new(DbType::Varchar(255), false, None)),
        ]));
        assert_eq!(287, table2.row_length());
    }

    // #[test]
    // fn write_tuple() {
    //     let schema = vec![
    //         FieldSpec::new("id", TypeSpec::new(DbType::UInt64, false, None)),
    //         FieldSpec::new("name", TypeSpec::new(DbType::Varchar(12), false, None)),
    //     ];
    //     let table = Table::new("my_table", &schema);

    //     let tuple = Tuple::new()
    // }
}
