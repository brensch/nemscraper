/// Represents whether an event was Downloaded or Processed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum State {
    Downloaded,
    Processed,
}

impl State {
    pub fn as_str(&self) -> &str {
        match self {
            State::Downloaded => "Downloaded",
            State::Processed => "Processed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "downloaded" => Some(State::Downloaded),
            "processed" => Some(State::Processed),
            _ => None,
        }
    }
}
