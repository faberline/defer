//! Property tests for the `--declared-queues` parser. Tests only.

use defer::{parse_declared_queues, DeclaredQueue, QueueControlState, QueuePolicy};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn parse_never_panics_on_any_text(input in ".*") {
        let _ = parse_declared_queues(&input);
    }

    #[test]
    fn parse_never_panics_on_json_shaped_text(input in r#"[\[\]{}\",:a-z0-9 .tnulfrse-]{0,80}"#) {
        let _ = parse_declared_queues(&input);
    }

    #[test]
    fn a_valid_declaration_round_trips(
        names in proptest::collection::btree_set("[a-z][a-z0-9-]{0,15}", 0..6),
        paused in proptest::collection::vec(0u8..3, 6),
        max_in_flight in 1usize..500,
    ) {
        let declared: Vec<DeclaredQueue> = names
            .iter()
            .enumerate()
            .map(|(i, name)| DeclaredQueue {
                name: name.clone(),
                policy: QueuePolicy { max_in_flight, ..QueuePolicy::default() },
                control: match paused[i] {
                    0 => QueueControlState::Running,
                    1 => QueueControlState::Paused,
                    _ => QueueControlState::Disabled,
                },
            })
            .collect();
        let json = serde_json::to_string(&declared).unwrap();
        prop_assert_eq!(parse_declared_queues(&json).unwrap(), declared);
    }

    #[test]
    fn a_duplicate_name_is_always_refused(name in "[a-z]{1,8}") {
        let json = format!(r#"[{{"name":"{name}"}},{{"name":"{name}"}}]"#);
        let err = parse_declared_queues(&json).unwrap_err();
        prop_assert!(format!("{err:#}").contains("duplicate"), "error was: {err:#}");
    }

    #[test]
    fn a_blank_name_is_always_refused(spaces in " {0,5}") {
        let json = format!(r#"[{{"name":"{spaces}"}}]"#);
        let err = parse_declared_queues(&json).unwrap_err();
        prop_assert!(format!("{err:#}").contains("empty"), "error was: {err:#}");
    }
}
