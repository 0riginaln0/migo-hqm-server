//! Faceoff placement and player-position assignment for standard matches.

use std::collections::HashMap;
use std::f32::consts::PI;

use glam::Vec3;
use glamx::Rot3;

use super::{ALLOWED_POSITIONS, Faceoff, RinkSide};
use crate::game::{PlayerId, Rink, Team};

pub(super) struct FaceoffPositions {
    pub(super) center: Vec3,
    pub(super) red: HashMap<&'static str, (Vec3, Rot3)>,
    pub(super) blue: HashMap<&'static str, (Vec3, Rot3)>,
}

pub(super) fn assign_team_positions(
    players: &[PlayerId],
    preferred: &HashMap<PlayerId, &'static str>,
) -> HashMap<PlayerId, &'static str> {
    let mut available = ALLOWED_POSITIONS.to_vec();
    let mut assignments = HashMap::new();

    // The first player requesting an available position claims it.
    for &id in players {
        if let Some(position) = preferred
            .get(&id)
            .copied()
            .and_then(|position| take_position(&mut available, position))
        {
            assignments.insert(id, position);
        }
    }

    // Fill the remaining slots, preferring center before the declared position order.
    for &id in players {
        assignments.entry(id).or_insert_with(|| {
            take_position(&mut available, "C")
                .or_else(|| take_next_position(&mut available))
                .unwrap_or_else(|| preferred.get(&id).copied().unwrap_or("C"))
        });
    }

    // If no one requested center, move the first non-goalie there.
    if available.contains(&"C") {
        if let Some(&id) = players
            .iter()
            .find(|&&id| assignments[&id] != "G")
            .or_else(|| players.first())
        {
            assignments.insert(id, "C");
        }
    }

    assignments
}

fn take_position(
    available: &mut Vec<&'static str>,
    requested: &'static str,
) -> Option<&'static str> {
    available
        .iter()
        .position(|&position| position == requested)
        .map(|index| available.remove(index))
}

fn take_next_position(available: &mut Vec<&'static str>) -> Option<&'static str> {
    (!available.is_empty()).then(|| available.remove(0))
}

pub(super) fn faceoff_spot(
    rink: &Rink,
    spot: Faceoff,
    spawn_offset: f32,
    altitude: f32,
) -> FaceoffPositions {
    let width = rink.width;
    let length = rink.length;
    let center_x = width / 2.0;
    let center = match spot {
        Faceoff::Center => Vec3::new(center_x, 0.0, length / 2.0),
        Faceoff::Defensive(team, side) => Vec3::new(
            if side == RinkSide::Lower {
                center_x - 7.0
            } else {
                center_x + 7.0
            },
            0.0,
            if team == Team::Red {
                length - 10.0
            } else {
                10.0
            },
        ),
        Faceoff::Offside(team, side) => Vec3::new(
            if side == RinkSide::Lower {
                center_x - 7.0
            } else {
                center_x + 7.0
            },
            0.0,
            if team == Team::Red {
                length - (rink.blue_zone_blue_line.z + 1.5)
            } else {
                rink.blue_zone_blue_line.z + 1.5
            },
        ),
    };
    let make_positions = |team: Team| {
        let rotation = if team == Team::Red {
            Rot3::IDENTITY
        } else {
            Rot3::from_rotation_y(PI)
        };
        let defensive = if team == Team::Red {
            center.z > length - 11.0
        } else {
            center.z < 11.0
        };
        let close_left = if team == Team::Red {
            center.x < 9.0
        } else {
            center.x > width - 9.0
        };
        let close_right = if team == Team::Red {
            center.x > width - 9.0
        } else {
            center.x < 9.0
        };
        let winger_z = 4.0;
        let middle_z = 7.25;
        let defense_z = if defensive { 8.25 } else { 10.0 };
        let far_left = if close_left {
            (-6.5, 3.0)
        } else {
            (-10.0, winger_z)
        };
        let far_right = if close_right {
            (6.5, 3.0)
        } else {
            (10.0, winger_z)
        };
        let offsets = [
            ("C", 0.0, spawn_offset),
            ("LM", -2.0, middle_z),
            ("RM", 2.0, middle_z),
            ("LW", -5.0, winger_z),
            ("RW", 5.0, winger_z),
            ("LD", -2.0, defense_z),
            ("RD", 2.0, defense_z),
            (
                "LLM",
                if close_left && defensive { -3.0 } else { -5.0 },
                middle_z,
            ),
            (
                "RRM",
                if close_right && defensive { 3.0 } else { 5.0 },
                middle_z,
            ),
            (
                "LLD",
                if close_left && defensive { -3.0 } else { -5.0 },
                defense_z,
            ),
            (
                "RRD",
                if close_right && defensive { 3.0 } else { 5.0 },
                defense_z,
            ),
            ("CM", 0.0, middle_z),
            ("CD", 0.0, defense_z),
            ("LW2", -6.0, winger_z),
            ("RW2", 6.0, winger_z),
            ("LLW", far_left.0, far_left.1),
            ("RRW", far_right.0, far_right.1),
        ];
        let mut positions = HashMap::new();
        for (name, x, z) in offsets {
            positions.insert(
                name,
                (center + rotation * Vec3::new(x, altitude, z), rotation),
            );
        }
        let goalie = if team == Team::Red {
            Vec3::new(center_x, altitude, length - 5.0)
        } else {
            Vec3::new(center_x, altitude, 5.0)
        };
        positions.insert("G", (goalie, rotation));
        positions
    };
    FaceoffPositions {
        center,
        red: make_positions(Team::Red),
        blue: make_positions(Team::Blue),
    }
}
