
#[cfg(target_pointer_width = "64")]
const POINTER_SIZE: usize = 8;
#[cfg(target_pointer_width = "32")]
const POINTER_SIZE: usize = 4;

#[derive(Debug)]
struct Table {
    name: String,
    schema: Schema,
    fixed_data: Vec<u8>,
    variable_data: Vec<u8>,
}

impl Table {
    fn new(name: String, schema: Schema) -> Self {
        Table {
            name,
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
    fn new(name: String, type_spec: TypeSpec) -> Self {
        FieldSpec {
            name,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_row_length() {
        let table1 = Table::new("test 1".to_string(), vec![
            FieldSpec::new("id".to_string(), TypeSpec::new(DbType::UInt64, false, None)),
        ]);
        assert_eq!(8, table1.row_length());

        let table2 = Table::new("test 2".to_string(), vec![
            FieldSpec::new("id".to_string(), TypeSpec::new(DbType::UInt64, false, None)),
            FieldSpec::new("age".to_string(), TypeSpec::new(DbType::UInt32, false, None)),
            FieldSpec::new("is_active".to_string(), TypeSpec::new(DbType::Boolean, false, None)),
        ]);
        assert_eq!(13, table2.row_length());
    }
}
