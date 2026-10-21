use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::TargetId;

/// What a collector is and which commands it accepts (`docs/architecture.md` §3.4).
/// Command names are defined by the collector, e.g. `heap_snapshot` by the .NET collector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorInfo {
    /// Collector name, e.g. `dotnet`, `process`.
    pub name: String,
    #[serde(default)]
    pub commands: Vec<String>,
}

/// Correlates a [`Command`] with its [`CommandResult`] and the events it produces.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CommandId(pub String);

/// A request for a collector to act now (`docs/architecture.md` §3.5). Scheduling is the
/// host's job: a command is sent when it should run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub command_id: CommandId,
    pub target: TargetId,
    /// Command name from the collector's [`CollectorInfo::commands`].
    pub name: String,
    /// Who asked and why, e.g. `rule:gen2-growth` or `user`; recorded in the artifact.
    pub reason: String,
    /// Command-specific parameters, defined by the collector.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub params: Map<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Done,
    /// Refused before running, e.g. by a safety limit or an unknown command name.
    Rejected,
    Failed,
}

/// Outcome of a [`Command`]. Measurements (object counts, pause) go into the collector's
/// events, not here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResult {
    pub command_id: CommandId,
    pub status: CommandStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<ArtifactRef>,
    /// Why the command was rejected or failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Reference to a self-contained artifact file (`docs/architecture.md` §3.6).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Artifact kind, defined by the collector, e.g. `briefcase`. Lets the artifact index
    /// filter without opening files.
    pub kind: String,
    /// File name, relative to the artifact store.
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn collector_info_round_trips() {
        let value = json!({ "name": "dotnet", "commands": ["heap_snapshot"] });
        let info: CollectorInfo = serde_json::from_value(value.clone()).expect("deserialize");
        assert_eq!(info.commands, ["heap_snapshot"]);
        assert_eq!(serde_json::to_value(&info).expect("serialize"), value);
    }

    #[test]
    fn collector_without_commands() {
        let info: CollectorInfo =
            serde_json::from_value(json!({ "name": "process" })).expect("deserialize");
        assert!(info.commands.is_empty());
    }

    #[test]
    fn command_round_trips_with_params() {
        let value = json!({
            "command_id": "c-42",
            "target": "pid:4812.1760843645@srv-a",
            "name": "heap_snapshot",
            "reason": "rule:gen2-growth",
            "params": { "max_heap_mb": 4096 }
        });
        let command: Command = serde_json::from_value(value.clone()).expect("deserialize");
        assert_eq!(command.name, "heap_snapshot");
        assert_eq!(command.params["max_heap_mb"], json!(4096));
        assert_eq!(serde_json::to_value(&command).expect("serialize"), value);
    }

    #[test]
    fn command_without_params_omits_them() {
        let value = json!({
            "command_id": "c-1", "target": "pid:1.1@h", "name": "cpu_profile", "reason": "user"
        });
        let command: Command = serde_json::from_value(value.clone()).expect("deserialize");
        assert!(command.params.is_empty());
        assert_eq!(serde_json::to_value(&command).expect("serialize"), value);
    }

    #[test]
    fn done_result_carries_artifact() {
        let value = json!({
            "command_id": "c-42",
            "status": "done",
            "artifact": { "kind": "briefcase", "path": "srv-a_4812_2026-10-19T03-14-05.briefcase" }
        });
        let result: CommandResult = serde_json::from_value(value.clone()).expect("deserialize");
        assert_eq!(result.status, CommandStatus::Done);
        assert_eq!(result.message, None);
        assert_eq!(serde_json::to_value(&result).expect("serialize"), value);
    }

    #[test]
    fn rejected_result_carries_message() {
        let value = json!({
            "command_id": "c-43",
            "status": "rejected",
            "message": "min_interval_s not elapsed"
        });
        let result: CommandResult = serde_json::from_value(value.clone()).expect("deserialize");
        assert_eq!(result.status, CommandStatus::Rejected);
        assert_eq!(result.artifact, None);
        assert_eq!(serde_json::to_value(&result).expect("serialize"), value);
    }

    #[test]
    fn unknown_status_is_an_error() {
        let result = serde_json::from_value::<CommandResult>(json!({
            "command_id": "c-1", "status": "exploded"
        }));
        assert!(result.is_err());
    }
}
