use std::{
    borrow::Cow,
    ops::{Index, IndexMut},
};

pub struct Lookup {
    size: usize,
    next_written: usize,
    last_read: usize,
    arr: Vec<Cow<'static, str>>,
}

#[derive(Debug, Copy, Clone)]
pub enum LookupType {
    Inc,
    Stay,
    Invalid,
}

static EMPTY: Cow<'static, str> = Cow::Borrowed("");

impl LookupType {
    fn apply(&self, value: usize) -> usize {
        match self {
            LookupType::Inc => value + 1,
            LookupType::Stay if value == 0 => 1,
            LookupType::Stay => value,
            LookupType::Invalid => panic!("Invalid lookup type, this should not happen with 0"),
        }
    }
}

impl Lookup {
    pub fn new(size: u32) -> Self {
        Self {
            arr: vec![Cow::Owned(String::new()); size as usize + 1],
            last_read: 0,
            next_written: 1,
            size: size as usize,
        }
    }

    pub fn get(&mut self, index: u32, ty: LookupType) -> &Cow<'static, str> {
        if self.size == 0 {
            return &EMPTY;
        }
        let mut id = index as usize;
        if index == 0 {
            id = ty.apply(self.last_read);
        }

        self.last_read = id;
        return &self.arr[id];
    }

    pub fn set(&mut self, index: u32, str: String) {
        let mut id = index as usize;
        if index == 0 {
            id = self.next_written;
        }
        self.next_written = id + 1;
        self.arr[id] = Cow::Owned(str);
    }
}

impl Index<usize> for Lookup {
    type Output = Cow<'static, str>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.arr[index - 1]
    }
}

impl IndexMut<usize> for Lookup {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let idx = index - 1;
        debug_assert!(idx <= self.arr.len());
        if idx == self.arr.len() {
            self.arr.push(Cow::Owned(String::new()));
        }
        return &mut self.arr[idx];
    }
}
