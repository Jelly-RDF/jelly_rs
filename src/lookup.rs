use std::{
    borrow::Cow,
    ops::{Index, IndexMut},
};

use crate::error::LookupError;

pub struct Lookup {
    size: usize,
    next_written: usize,
    last_read: usize,
    arr: Vec<Option<Cow<'static, str>>>,
}

#[derive(Debug, Copy, Clone)]
pub enum LookupType {
    Inc,
    Stay,
    Invalid,
}

impl LookupType {
    fn apply(&self, value: usize) -> Result<usize, LookupError> {
        Ok(match self {
            LookupType::Inc => value + 1,
            LookupType::Stay if value == 0 => 1,
            LookupType::Stay => value,
            LookupType::Invalid => return Err(LookupError::InvalidLookupAction),
        })
    }
}

impl Lookup {
    pub fn new(size: u32) -> Self {
        Self {
            arr: vec![None; size as usize + 1],
            last_read: 0,
            next_written: 1,
            size: size as usize,
        }
    }

    pub fn get(&mut self, index: u32, ty: LookupType) -> Result<&Cow<'static, str>, LookupError> {
        if self.size == 0 {
            return Err(LookupError::LookupFromEmptyTable);
        }
        let mut id = index as usize;
        if index == 0 {
            id = ty.apply(self.last_read)?;
        }

        self.last_read = id;
        self.arr
            .get(id)
            .and_then(|x| x.as_ref())
            .ok_or_else(|| LookupError::Missing(id, self.arr.len()))
    }

    pub fn set(&mut self, index: u32, str: String) -> Result<(), LookupError> {
        let mut id = index as usize;
        if index == 0 {
            id = self.next_written;
        }
        self.next_written = id + 1;
        if id > self.size {
            return Err(LookupError::LookupTableTooSmall(self.size));
        }

        self.arr[id] = Some(Cow::Owned(str));
        Ok(())
    }
}

impl Index<usize> for Lookup {
    type Output = Option<Cow<'static, str>>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.arr[index - 1]
    }
}

impl IndexMut<usize> for Lookup {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        return &mut self.arr[index - 1];
    }
}
