// HANDWRITE-BEGIN gap="missing-generator:logic:defer-queue-placement" tracker="#83" reason="Queue-keyed shard placement hook over shard-routing."
//! Queue-keyed shard placement: a queue name picks its shard. Pure: no I/O,
//! no clock.

use shard_routing::{
    bucket_moves, BucketMove, RequestRouting, RouteDecision, VirtualBucketShardMap,
};

/// The `collection_id` of every placement call. A queue is the placement key.
pub const PLACEMENT_COLLECTION: &str = "queue";

/// One request. `None` means the request spans queues (for example `GET /v1/queues`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRequest {
    pub queue: Option<String>,
}

/// The shard map as seen by one physical shard.
#[derive(Debug, Clone)]
pub struct QueuePlacement {
    map: VirtualBucketShardMap,
    this_shard: u32,
}

impl QueuePlacement {
    pub fn new(map: VirtualBucketShardMap, this_shard: u32) -> Self {
        Self { map, this_shard }
    }

    pub fn shard_of(&self, queue: &str) -> u32 {
        self.map.route_key(PLACEMENT_COLLECTION, queue).shard
    }

    pub fn bucket_of(&self, queue: &str) -> u32 {
        self.map.route_key(PLACEMENT_COLLECTION, queue).bucket
    }

    /// For a split to `new_map`: each queue of `queues` whose bucket moves,
    /// with that move, in the order of `queues`.
    pub fn queues_to_move(
        &self,
        new_map: &VirtualBucketShardMap,
        queues: &[String],
    ) -> anyhow::Result<Vec<(String, BucketMove)>> {
        let moves = bucket_moves(&self.map, new_map)?;
        Ok(queues
            .iter()
            .filter_map(|queue| {
                let bucket = self.bucket_of(queue);
                moves
                    .iter()
                    .find(|m| m.bucket == bucket)
                    .map(|m| (queue.clone(), *m))
            })
            .collect())
    }
}

impl RequestRouting for QueuePlacement {
    type Request = QueueRequest;
    type Response = Vec<String>;

    fn classify(&self, request: &QueueRequest) -> RouteDecision {
        match request.queue.as_deref() {
            None => RouteDecision::Scatter,
            Some(queue) => {
                let shard = self.shard_of(queue);
                if shard == self.this_shard {
                    RouteDecision::Local
                } else {
                    RouteDecision::Owner(shard)
                }
            }
        }
    }

    /// A list endpoint: the queue names of every shard, in name order, once each.
    fn merge(&self, parts: Vec<Vec<String>>) -> Vec<String> {
        let mut names: Vec<String> = parts.into_iter().flatten().collect();
        names.sort();
        names.dedup();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shard_routing::{VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT};

    /// One shard, then the same map split to two shards.
    fn maps() -> (VirtualBucketShardMap, VirtualBucketShardMap) {
        let old = VirtualBucketShardMap::balanced(1, DEFAULT_VIRTUAL_BUCKET_COUNT, 1).unwrap();
        let new = old.split_one_shard(2).unwrap();
        (old, new)
    }

    fn queue_on(map: &VirtualBucketShardMap, shard: u32) -> String {
        (0..1000)
            .map(|i| format!("q{i}"))
            .find(|q| map.route_key(PLACEMENT_COLLECTION, q).shard == shard)
            .unwrap_or_else(|| panic!("no queue on shard {shard} among q0..q999"))
    }

    #[test]
    fn classify_is_local_for_the_owning_shard() {
        let (_, new) = maps();
        let stayed = queue_on(&new, 0);
        let moved = queue_on(&new, 1);
        let placement = QueuePlacement::new(new.clone(), 0);

        assert_eq!(
            placement.classify(&QueueRequest {
                queue: Some(stayed)
            }),
            RouteDecision::Local
        );
        assert_eq!(
            placement.classify(&QueueRequest { queue: Some(moved) }),
            RouteDecision::Owner(1)
        );
    }

    #[test]
    fn no_queue_means_scatter() {
        let (old, _) = maps();
        let placement = QueuePlacement::new(old, 0);
        assert_eq!(
            placement.classify(&QueueRequest { queue: None }),
            RouteDecision::Scatter
        );
    }

    #[test]
    fn merge_sorts_and_dedups() {
        let (old, _) = maps();
        let placement = QueuePlacement::new(old, 0);
        let merged = placement.merge(vec![
            vec!["b".into(), "a".into()],
            vec!["c".into(), "a".into()],
        ]);
        assert_eq!(merged, vec!["a".to_string(), "b".into(), "c".into()]);
    }

    #[test]
    fn queues_to_move_lists_exactly_the_moved_queues() {
        let (old, new) = maps();
        let names: Vec<String> = (0..100).map(|i| format!("q{i}")).collect();
        let placement = QueuePlacement::new(old.clone(), 0);

        let moves = placement.queues_to_move(&new, &names).unwrap();
        let expected: Vec<&String> = names
            .iter()
            .filter(|q| new.route_key(PLACEMENT_COLLECTION, q).shard == 1)
            .collect();
        let got: Vec<&String> = moves.iter().map(|(q, _)| q).collect();

        assert!(!moves.is_empty());
        assert!(moves.len() < names.len());
        assert_eq!(got, expected);
        for (_, bucket_move) in &moves {
            assert_eq!(bucket_move.from_shard, 0);
            assert_eq!(bucket_move.to_shard, 1);
        }

        // Same map on both sides: nothing moves.
        assert!(placement
            .queues_to_move(&old.clone(), &names)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn placement_is_stable_for_the_same_queue() {
        let (old, new) = maps();
        let placement = QueuePlacement::new(new, 0);
        assert_eq!(placement.shard_of("jobs"), placement.shard_of("jobs"));

        let single = QueuePlacement::new(old, 0);
        assert_eq!(single.shard_of("jobs"), 0);
        assert_eq!(single.shard_of("anything"), 0);
    }
}
// HANDWRITE-END
