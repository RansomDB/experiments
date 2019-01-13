use byteorder::{ByteOrder, LittleEndian};
use std::ptr;
use std::ops::Deref;

#[cfg(target_pointer_width = "64")]
const POINTER_SIZE: usize = 8;
#[cfg(target_pointer_width = "32")]
const POINTER_SIZE: usize = 4;

#[derive(Debug)]
struct DbHeap {
    buf: Vec<u8>,
}

impl DbHeap {

    fn new() -> Self {
        DbHeap {
            buf: vec![]
        }
    }

    // Create a buffer with an initial size for its internal
    // byte buffer.
    fn new_sized(size: usize) -> Self {
        DbHeap {
            buf: Vec::with_capacity(size)
        }
    }

    // Adds data to internal memory and returns the starting offset
    // at which the data resides
    fn append_data(&mut self, data: &mut Vec<u8>) -> usize {
        let prev_len = self.buf.len();
        self.buf.append(data);

        prev_len
    }

    fn get_slice(&self, offset: usize, len: usize) -> &[u8] {
        &self.buf[offset..(offset+len)]
    }
}

trait DbValue {
    fn size(&self) -> usize;
    fn read_from_buffer(&mut self, buf: &[u8], heap: &DbHeap);
    fn write_to_buffer(&self, buf: &mut [u8], heap: &mut DbHeap);
}

#[derive(Debug, PartialEq, Eq)]
struct DBUInt64(u64);

impl DBUInt64 {
    fn new() -> Self {
        DBUInt64(0)
    }
}

impl Deref for DBUInt64 {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DbValue for DBUInt64 {
    fn size(&self) -> usize {
        8
    }

    fn read_from_buffer(&mut self, buf: &[u8], _heap: & DbHeap) {
        self.0 = LittleEndian::read_u64(buf);
    }

    fn write_to_buffer(&self, buf: &mut [u8], _heap: &mut DbHeap) {
        LittleEndian::write_u64(buf, self.0);
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DBUInt32(u32);

impl DBUInt32 {
    fn new() -> Self {
        DBUInt32(0)
    }
}

impl Deref for DBUInt32 {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DbValue for DBUInt32 {
    fn size(&self) -> usize {
        4
    }

    fn read_from_buffer(&mut self, buf: &[u8], _heap: &DbHeap) {
        self.0 = LittleEndian::read_u32(buf);
    }

    fn write_to_buffer(&self, buf: &mut [u8], _heap: &mut DbHeap) {
        LittleEndian::write_u32(buf, self.0);
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DBBoolean(bool);

impl DBBoolean {
    fn new() -> Self {
        DBBoolean(false)
    }
}

impl DbValue for DBBoolean {
    fn size(&self) -> usize {
        1
    }

    fn read_from_buffer(&mut self, buf: &[u8], _heap: &DbHeap) {
        self.0 = buf[0] == 1;
    }

    fn write_to_buffer(&self, buf: &mut [u8], _heap: &mut DbHeap) {
        buf[0] = if self.0 {
            1
        } else {
            0
        };
    }
}

impl Deref for DBBoolean {
    type Target = bool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DBInlineString(String);

impl DBInlineString {
    fn new() -> Self {
        DBInlineString("".to_string())
    }
}

impl DbValue for DBInlineString {
    fn size(&self) -> usize {
        1 + self.0.len()
    }

    fn read_from_buffer(&mut self, buf: &[u8], _heap: &DbHeap) {
        let size = buf[0];
        let data = &buf[1..(size as usize + 1)];
        self.0 = String::from_utf8_lossy(data).to_string();
    }

    fn write_to_buffer(&self, buf: &mut [u8], _heap: &mut DbHeap) {
        let data_size = self.0.len();
        let (size_buf, data_buf) = buf.split_at_mut(1);
        let src_ptr = self.0.as_bytes().as_ptr() as *const u8;
        size_buf[0] = data_size as u8;
        unsafe {
            ptr::copy(src_ptr, data_buf.as_mut_ptr(), data_size);
        }
    }
}

impl Deref for DBInlineString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DBExternalString (String);

impl DBExternalString {
    fn new() -> Self {
        DBExternalString("".to_string())
    }
}

impl DbValue for DBExternalString {
    fn size(&self) -> usize {
        std::mem::size_of::<usize>() + self.0.len()
    }

    #[cfg(target_pointer_width = "64")]
    fn read_from_buffer(&mut self, buf: &[u8], heap: &DbHeap) {
        let offset = LittleEndian::read_u64(buf) as usize;
        let data_start_offset = offset + 8;
        let size = LittleEndian::read_u64(heap.get_slice(offset, 8));
        let data = heap.get_slice(data_start_offset, size as usize);
        self.0 = String::from_utf8_lossy(data).to_string();
    }

    #[cfg(target_pointer_width = "32")]
    fn read_from_buffer(&mut self, buf: &[u8], heap: &DbHeap) {
        let offset = LittleEndian::read_u32(buf) as usize;
        let data_start_offset = offset + 4;
        let size = LittleEndian::read_u32(heap.get_slice(offset, 4));
        let data = heap.get_slice(data_start_offset, size as usize);
        self.0 = String::from_utf8_lossy(data).to_string();
    }

    #[cfg(target_pointer_width = "64")]
    fn write_to_buffer(&self, buf: &mut [u8], heap: &mut DbHeap) {
        let mut size_buf: [u8; 8] = [0; 8];
        LittleEndian::write_u64(&mut size_buf, self.0.len() as u64);
        let mut len_prefixed_string = vec![];
        len_prefixed_string.extend_from_slice(&mut size_buf);
        len_prefixed_string.extend_from_slice(self.0.clone().as_bytes());

        let offset = heap.append_data(&mut len_prefixed_string);
        LittleEndian::write_u64(buf, offset as u64);
    }
}

impl Deref for DBExternalString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint64_serialize() {
        let mut heap_unused = DbHeap::new();

        let test_cases: Vec<u64> = vec![0, u64::max_value(), 4538756723];
        for x in test_cases {
            let val = DBUInt64(x);
            let mut new_val = DBUInt64::new();
            let mut buf = [0u8; 8];

            val.write_to_buffer(&mut buf, &mut heap_unused);
            new_val.read_from_buffer(&buf, &heap_unused);

            assert_eq!(val, new_val);
        }
    }

    #[test]
    fn uint32_serialize() {
        let mut heap_unused = DbHeap::new();

        let test_cases: Vec<u32> = vec![0, u32::max_value(), 4538756];
        for x in test_cases {
            let val = DBUInt32(x);
            let mut new_val = DBUInt32::new();
            let mut buf = [0u8; 4];

            val.write_to_buffer(&mut buf, &mut heap_unused);
            new_val.read_from_buffer(&buf, &heap_unused);

            assert_eq!(val, new_val);
        }
    }

    #[test]
    fn boolean_serialize() {
        let mut heap_unused = DbHeap::new();

        let test_cases: Vec<bool> = vec![true, false];
        for p in test_cases {
            let val = DBBoolean(p);
            let mut new_val = DBBoolean::new();
            let mut buf = [0u8; 1];

            val.write_to_buffer(&mut buf, &mut heap_unused);
            new_val.read_from_buffer(&buf, &heap_unused);

            assert_eq!(val, new_val);
        }
    }

    #[test]
    fn inline_string_serialize() {
        let mut heap_unused = DbHeap::new();

        let test_cases: Vec<String> = vec![
            "".to_string(),
            " ".to_string(),
            "abc123!@#\n\t".to_string(),
            "Infinite Taco -> ∞ 🌮".to_string(),
        ];
        for s in test_cases {
            let val = DBInlineString(s);
            let mut new_val = DBInlineString::new();
            let mut buf = [0u8; 32];

            val.write_to_buffer(&mut buf, &mut heap_unused);
            new_val.read_from_buffer(&buf, &heap_unused);

            assert_eq!(val, new_val);
        }
    }

    #[test]
    fn external_string_serialize() {
        let mut heap = DbHeap::new();

        let test_cases: Vec<String> = vec![
            "".to_string(),
            " ".to_string(),
            "abc123!@#\n\t".to_string(),
            "Infinite Taco -> ∞ 🌮".to_string(),
        ];
        for s in test_cases {
            let val = DBExternalString(s);
            let mut new_val = DBExternalString::new();
            let mut buf = [0u8; 8];

            val.write_to_buffer(&mut buf, &mut heap);
            new_val.read_from_buffer(&buf, &heap);

            assert_eq!(val, new_val);
        }
    }
}