use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueKind {
    Put(Bytes),
    Delete,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_variant_holds_bytes() {
        let data = Bytes::from("hello");
        let vk = ValueKind::Put(data.clone());
        match vk {
            ValueKind::Put(v) => assert_eq!(v, data),
            ValueKind::Delete => panic!("expected Put"),
        }
    }

    #[test]
    fn delete_variant_is_distinct_from_put() {
        let vk = ValueKind::Delete;
        assert!(matches!(vk, ValueKind::Delete));
    }

    #[test]
    fn put_with_empty_bytes_is_not_a_tombstone() {
        let vk = ValueKind::Put(Bytes::new());
        assert!(matches!(vk, ValueKind::Put(_)));
    }

    #[test]
    fn valuekind_is_exported_from_crate() {
        let _: crate::types::ValueKind = ValueKind::Delete;
    }
}
