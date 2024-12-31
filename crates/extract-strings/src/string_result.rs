// #[derive(Debug)]
// pub enum StringResult {
//     Complete(String),
//     Partial(String),
// }
//
// impl StringResult {
//     pub fn complete(s: &[u8]) -> Self {
//         let s = std::str::from_utf8(s).unwrap();
//         Self::Complete(s.to_string())
//     }
//
//     pub fn partial(s: &[u8]) -> Self {
//         let s = std::str::from_utf8(s).unwrap();
//         Self::Partial(s.to_string())
//     }
//
//     pub fn into_string(self) -> String {
//         match self {
//             Self::Complete(s) | Self::Partial(s) => s,
//         }
//     }
//
//     pub fn str(&self) -> &str {
//         match self {
//             Self::Complete(s) | Self::Partial(s) => s,
//         }
//     }
//
//     pub fn is_partial(&self) -> bool {
//         matches!(self, Self::Partial(_))
//     }
// }
//
// impl<T> PartialEq<T> for StringResult
// where
//     T: AsRef<str>,
// {
//     fn eq(&self, other: &T) -> bool {
//         self.str() == other.as_ref()
//     }
// }
