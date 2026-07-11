use crate::game::{PlayerId, PlayerIndex, PlayerInput, SkaterHand, Team};
use crate::server::{HQMClientVersion, ObjectSlot, PacketNumber};
use arraydeque::ArrayDeque;
use arraydeque::behavior::Wrapping;
use std::borrow::Cow;
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::info;

#[derive(Debug, Clone)]
pub(crate) struct PlayerUpdateData {
    pub player_name: Rc<str>,
    pub object: Option<(ObjectSlot, Team)>,
}

#[derive(Debug, Clone)]
pub(crate) enum HQMMessage {
    PlayerUpdate {
        player_index: PlayerIndex,
        data: Option<PlayerUpdateData>,
    },
    Goal {
        team: Team,
        goal_player_index: Option<PlayerIndex>,
        assist_player_index: Option<PlayerIndex>,
    },
    Chat {
        player_index: Option<PlayerIndex>,
        message: Cow<'static, str>,
    },
}

pub(crate) trait PlayerListExt {
    fn get_player_by_index(&self, player_index: PlayerIndex) -> Option<(PlayerId, &ServerPlayer)>;

    fn get_player(&self, player_id: PlayerId) -> Option<&ServerPlayer>;

    fn get_player_mut_by_index(
        &mut self,
        player_index: PlayerIndex,
    ) -> Option<(PlayerId, &mut ServerPlayer)>;

    fn get_player_mut(&mut self, player_id: PlayerId) -> Option<&mut ServerPlayer>;
    fn iter_players(&self) -> impl Iterator<Item = (PlayerId, &ServerPlayer)>;

    fn iter_players_mut(&mut self) -> impl Iterator<Item = (PlayerId, &mut ServerPlayer)>;

    fn check_admin_or_deny(&mut self, player_id: PlayerId) -> Option<&ServerPlayer> {
        if let Some(player) = self.get_player_mut(player_id) {
            if player.is_admin {
                Some(player)
            } else {
                player.add_directed_server_chat_message("Please log in before using that command");
                None
            }
        } else {
            None
        }
    }

    fn find_player_by_addr(&self, addr: SocketAddr) -> Option<(PlayerId, &ServerPlayer)> {
        self.iter_players().find(|(_, x)| {
            if let ServerPlayerData::NetworkPlayer { data } = &x.data {
                data.addr == addr
            } else {
                false
            }
        })
    }

    fn find_player_by_addr_mut(
        &mut self,
        addr: SocketAddr,
    ) -> Option<(PlayerId, &mut ServerPlayer)> {
        self.iter_players_mut().find(|(_, x)| {
            if let ServerPlayerData::NetworkPlayer { data } = &x.data {
                data.addr == addr
            } else {
                false
            }
        })
    }

    fn find_empty_slot(&self) -> Option<PlayerIndex>;
}

pub(crate) const MAX_PLAYERS: usize = 64;

struct PlayerSlot {
    generation: u32,
    player: Option<ServerPlayer>,
}

/// Fixed-size player storage with generation-checked player identifiers.
pub(crate) struct PlayerSlots {
    slots: [PlayerSlot; MAX_PLAYERS],
}

impl PlayerSlots {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| PlayerSlot {
                generation: 0,
                player: None,
            }),
        }
    }

    fn insert(&mut self, index: PlayerIndex, player: ServerPlayer) -> PlayerId {
        let slot = &mut self.slots[index.0];
        debug_assert!(slot.player.is_none());
        slot.player = Some(player);
        PlayerId {
            index,
            counter: slot.generation,
        }
    }

    pub(crate) fn remove(&mut self, player_id: PlayerId) -> Option<ServerPlayer> {
        let slot = self.slots.get_mut(player_id.index.0)?;
        if slot.generation != player_id.counter {
            return None;
        }

        let player = slot.player.take()?;
        slot.generation = slot.generation.wrapping_add(1);
        Some(player)
    }
}

impl PlayerListExt for PlayerSlots {
    fn get_player_by_index(&self, player_index: PlayerIndex) -> Option<(PlayerId, &ServerPlayer)> {
        self.slots.get(player_index.0).and_then(|slot| {
            slot.player.as_ref().map(|player| {
                (
                    PlayerId {
                        index: player_index,
                        counter: slot.generation,
                    },
                    player,
                )
            })
        })
    }

    fn get_player(&self, player_id: PlayerId) -> Option<&ServerPlayer> {
        self.slots
            .get(player_id.index.0)
            .and_then(|slot| match &slot.player {
                Some(player) if slot.generation == player_id.counter => Some(player),
                _ => None,
            })
    }

    fn get_player_mut_by_index(
        &mut self,
        player_index: PlayerIndex,
    ) -> Option<(PlayerId, &mut ServerPlayer)> {
        self.slots.get_mut(player_index.0).and_then(|slot| {
            slot.player.as_mut().map(|player| {
                (
                    PlayerId {
                        index: player_index,
                        counter: slot.generation,
                    },
                    player,
                )
            })
        })
    }

    fn get_player_mut(&mut self, player_id: PlayerId) -> Option<&mut ServerPlayer> {
        self.slots
            .get_mut(player_id.index.0)
            .and_then(|slot| match &mut slot.player {
                Some(player) if slot.generation == player_id.counter => Some(player),
                _ => None,
            })
    }

    fn iter_players(&self) -> impl Iterator<Item = (PlayerId, &ServerPlayer)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(player_index, slot)| {
                slot.player.as_ref().map(|player| {
                    (
                        PlayerId {
                            index: PlayerIndex(player_index),
                            counter: slot.generation,
                        },
                        player,
                    )
                })
            })
    }

    fn iter_players_mut(&mut self) -> impl Iterator<Item = (PlayerId, &mut ServerPlayer)> {
        self.slots
            .iter_mut()
            .enumerate()
            .filter_map(|(player_index, slot)| {
                slot.player.as_mut().map(|player| {
                    (
                        PlayerId {
                            index: PlayerIndex(player_index),
                            counter: slot.generation,
                        },
                        player,
                    )
                })
            })
    }

    fn find_empty_slot(&self) -> Option<PlayerIndex> {
        self.slots
            .iter()
            .position(|slot| slot.player.is_none())
            .map(|x| PlayerIndex(x))
    }
}

#[derive(Clone, Copy)]
pub(crate) enum MessageRetention {
    Transient,
    ForLateJoiners,
}

#[derive(Clone, Copy)]
pub(crate) enum MessageRecording {
    Exclude,
    Include,
}

pub(crate) struct ServerPlayersAndMessages {
    pub(crate) players: PlayerSlots,

    persistent_messages: Vec<Rc<HQMMessage>>,
    recording_messages: Vec<Rc<HQMMessage>>,
}

impl ServerPlayersAndMessages {
    pub(crate) fn new() -> Self {
        Self {
            players: PlayerSlots::new(),
            persistent_messages: vec![],
            recording_messages: vec![],
        }
    }

    pub(crate) fn new_game(&mut self) {
        self.recording_messages.clear();
        self.persistent_messages.clear();

        let mut messages = Vec::new();
        for (player_id, player) in self.players.iter_players_mut() {
            player.reset(player_id.index);
            messages.push(player.get_update_message(player_id.index));
        }

        for message in messages {
            self.broadcast_message(
                message,
                MessageRetention::ForLateJoiners,
                MessageRecording::Include,
            );
        }
    }

    pub(crate) fn add_user_chat_message(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        sender_index: PlayerIndex,
    ) {
        let chat = HQMMessage::Chat {
            player_index: Some(sender_index),
            message: message.into(),
        };
        self.broadcast_message(chat, MessageRetention::Transient, MessageRecording::Include);
    }

    pub(crate) fn add_server_chat_message(&mut self, message: impl Into<Cow<'static, str>>) {
        let chat = HQMMessage::Chat {
            player_index: None,
            message: message.into(),
        };
        self.broadcast_message(chat, MessageRetention::Transient, MessageRecording::Include);
    }

    pub fn add_directed_chat_message(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        receiver_id: PlayerId,
        sender_index: Option<PlayerIndex>,
    ) {
        if let Some(player) = self.players.get_player_mut(receiver_id) {
            player.add_directed_chat_message(message, sender_index)
        }
    }

    pub fn add_directed_user_chat_message(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        receiver_id: PlayerId,
        sender_index: PlayerIndex,
    ) {
        self.add_directed_chat_message(message, receiver_id, Some(sender_index));
    }

    pub fn add_directed_server_chat_message(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        receiver_id: PlayerId,
    ) {
        self.add_directed_chat_message(message, receiver_id, None);
    }

    pub fn add_goal_message(
        &mut self,
        team: Team,
        goal_player_index: Option<PlayerId>,
        assist_player_index: Option<PlayerId>,
    ) {
        let goal_player_index = goal_player_index.and_then(|x| {
            if self.players.get_player(x).is_some() {
                Some(x.index)
            } else {
                None
            }
        });
        let assist_player_index = assist_player_index.and_then(|x| {
            if self.players.get_player(x).is_some() {
                Some(x.index)
            } else {
                None
            }
        });
        let message = HQMMessage::Goal {
            team,
            goal_player_index,
            assist_player_index,
        };
        self.broadcast_message(
            message,
            MessageRetention::ForLateJoiners,
            MessageRecording::Include,
        );
    }

    pub(crate) fn broadcast_message(
        &mut self,
        message: HQMMessage,
        retention: MessageRetention,
        recording: MessageRecording,
    ) {
        let rc = Rc::new(message);
        if matches!(recording, MessageRecording::Include) {
            self.recording_messages.push(rc.clone());
        }
        if matches!(retention, MessageRetention::ForLateJoiners) {
            self.persistent_messages.push(rc.clone());
        }
        for (_, player) in self.players.iter_players_mut() {
            player.add_message(rc.clone());
        }
    }

    pub(crate) fn add_user_team_message(&mut self, message: &str, sender_id: PlayerId) {
        if let Some(player) = self.players.get_player(sender_id) {
            let team = if let Some((_, team)) = player.object {
                Some(team)
            } else {
                None
            };
            if let Some(team) = team {
                info!(
                    "{} ({}) to team {}: {}",
                    &player.player_name, sender_id, team, message
                );
                let object = player
                    .object
                    .as_ref()
                    .map(|(object_index, team)| (*object_index, *team));

                let team_tag_name = match team {
                    Team::Red => player.player_name_red.clone(),
                    Team::Blue => player.player_name_blue.clone(),
                };

                let change1 = Rc::new(HQMMessage::PlayerUpdate {
                    player_index: sender_id.index,
                    data: Some(PlayerUpdateData {
                        player_name: team_tag_name,
                        object,
                    }),
                });
                let change2 = Rc::new(HQMMessage::PlayerUpdate {
                    player_index: sender_id.index,
                    data: Some(PlayerUpdateData {
                        player_name: player.player_name.clone(),
                        object,
                    }),
                });
                let chat = Rc::new(HQMMessage::Chat {
                    player_index: Some(sender_id.index),
                    message: Cow::Owned(message.to_owned()),
                });

                self.send_to_team(team, &[change1, chat, change2]);
            }
        }
    }

    fn send_to_team(&mut self, team: Team, messages: &[Rc<HQMMessage>]) {
        for (_, player) in self.players.iter_players_mut() {
            if player.team().is_some_and(|player_team| player_team == team) {
                for message in messages {
                    player.add_message(message.clone());
                }
            }
        }
    }

    pub(crate) fn add_player(&mut self, player_name: &str, addr: SocketAddr) -> Option<PlayerId> {
        if self.players.find_player_by_addr(addr).is_some() {
            return None;
        }
        let player_index = self.players.find_empty_slot();
        match player_index {
            Some(player_index) => {
                let new_player = ServerPlayer::new_network_player(
                    player_index,
                    player_name,
                    addr,
                    &self.persistent_messages,
                );
                let update = new_player.get_update_message(player_index);

                let player_id = self.players.insert(player_index, new_player);

                self.broadcast_message(
                    update,
                    MessageRetention::ForLateJoiners,
                    MessageRecording::Include,
                );

                Some(player_id)
            }
            _ => None,
        }
    }

    pub(crate) fn add_bot(&mut self, player_name: &str) -> Option<PlayerId> {
        let player_index = self.players.find_empty_slot();
        match player_index {
            Some(player_index) => {
                let new_player = ServerPlayer::new_bot(player_name);
                let update = new_player.get_update_message(player_index);

                let player_id = self.players.insert(player_index, new_player);

                self.broadcast_message(
                    update,
                    MessageRetention::ForLateJoiners,
                    MessageRecording::Include,
                );

                Some(player_id)
            }
            _ => None,
        }
    }

    pub(crate) fn recording_messages_since(&self, message_pos: usize) -> &[Rc<HQMMessage>] {
        &self.recording_messages[message_pos..]
    }

    pub(crate) fn recording_message_count(&self) -> usize {
        self.recording_messages.len()
    }
}

pub(crate) struct NetworkPlayerData {
    pub addr: SocketAddr,
    pub client_version: HQMClientVersion,
    pub(crate) inactivity: u32,
    pub known_packet: Option<PacketNumber>,
    pub known_msgpos: usize,
    pub(crate) chat_rep: Option<u8>,
    pub deltatime: u32,
    pub(crate) last_ping: Box<ArrayDeque<f32, 100, Wrapping>>,
    pub view_player_index: PlayerIndex,
    pub game_id: u32,
    pub messages: Vec<Rc<HQMMessage>>,
}

pub(crate) enum ServerPlayerData {
    NetworkPlayer { data: NetworkPlayerData },
    Bot {},
}

pub struct ServerPlayer {
    pub player_name: Rc<str>,
    player_name_red: Rc<str>,
    player_name_blue: Rc<str>,
    object: Option<(ObjectSlot, Team)>,
    pub data: ServerPlayerData,
    pub is_admin: bool,
    pub is_muted: MuteStatus,
    pub preferred_hand: SkaterHand,
    pub input: PlayerInput,
}

impl ServerPlayer {
    pub fn new_network_player(
        player_index: PlayerIndex,
        player_name: &str,
        addr: SocketAddr,
        global_messages: &[Rc<HQMMessage>],
    ) -> Self {
        ServerPlayer {
            player_name: player_name.into(),
            player_name_red: format!("[Red] {player_name}").into(),
            player_name_blue: format!("[Blue] {player_name}").into(),
            object: None,
            data: ServerPlayerData::NetworkPlayer {
                data: NetworkPlayerData {
                    addr,
                    client_version: HQMClientVersion::Vanilla,
                    inactivity: 0,
                    known_packet: None,
                    known_msgpos: 0,
                    chat_rep: None,
                    // store latest deltime client sends you to respond with it
                    deltatime: 0,
                    last_ping: Box::new(ArrayDeque::new()),
                    view_player_index: player_index,
                    game_id: u32::MAX,
                    messages: global_messages.to_vec(),
                },
            },
            is_admin: false,
            input: Default::default(),
            is_muted: MuteStatus::NotMuted,
            preferred_hand: SkaterHand::Right,
        }
    }

    pub fn new_bot(player_name: &str) -> Self {
        ServerPlayer {
            player_name: player_name.into(),
            player_name_red: format!("[Red] {player_name}").into(),
            player_name_blue: format!("[Blue] {player_name}").into(),
            object: None,
            data: ServerPlayerData::Bot {},
            is_admin: false,
            input: Default::default(),
            is_muted: MuteStatus::NotMuted,
            preferred_hand: SkaterHand::Right,
        }
    }

    fn reset(&mut self, player_index: PlayerIndex) {
        self.object = None;
        if let ServerPlayerData::NetworkPlayer { data } = &mut self.data {
            data.known_msgpos = 0;
            data.known_packet = None;
            data.messages.clear();
            data.view_player_index = player_index;
        }
    }

    pub(crate) fn get_update_message(&self, player_index: PlayerIndex) -> HQMMessage {
        HQMMessage::PlayerUpdate {
            player_index,
            data: Some(PlayerUpdateData {
                player_name: self.player_name.clone(),
                object: self
                    .object
                    .as_ref()
                    .map(|(object_slot, team)| (*object_slot, *team)),
            }),
        }
    }

    fn add_message(&mut self, message: Rc<HQMMessage>) {
        if let ServerPlayerData::NetworkPlayer {
            data: NetworkPlayerData { messages, .. },
        } = &mut self.data
        {
            messages.push(message);
        }
    }

    pub(crate) fn ping_data(&self) -> Option<PingData> {
        match self.data {
            ServerPlayerData::NetworkPlayer {
                data: NetworkPlayerData { ref last_ping, .. },
            } => {
                let n = last_ping.len() as f32;
                let mut min = f32::INFINITY;
                let mut max = f32::NEG_INFINITY;
                let mut sum = 0f32;
                for i in last_ping.iter() {
                    min = min.min(*i);
                    max = max.max(*i);
                    sum += *i;
                }
                let avg = sum / n;
                let dev = {
                    let mut s = 0f32;
                    for i in last_ping.iter() {
                        s += (*i - avg).powi(2);
                    }
                    (s / n).sqrt()
                };
                Some(PingData {
                    min,
                    max,
                    avg,
                    deviation: dev,
                })
            }
            ServerPlayerData::Bot { .. } => None,
        }
    }

    pub fn add_directed_user_chat_message(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        sender_index: PlayerIndex,
    ) {
        self.add_directed_chat_message(message, Some(sender_index));
    }

    pub fn add_directed_server_chat_message(&mut self, message: impl Into<Cow<'static, str>>) {
        self.add_directed_chat_message(message, None);
    }

    pub fn add_directed_chat_message(
        &mut self,
        message: impl Into<Cow<'static, str>>,
        sender_index: Option<PlayerIndex>,
    ) {
        let chat = HQMMessage::Chat {
            player_index: sender_index,
            message: message.into(),
        };
        self.add_message(Rc::new(chat));
    }

    pub fn team(&self) -> Option<Team> {
        self.object.as_ref().map(|x| x.1)
    }

    pub fn has_skater(&self) -> bool {
        self.object.is_some()
    }

    pub(crate) fn skater_assignment(&self) -> Option<(ObjectSlot, Team)> {
        self.object
    }

    pub(crate) fn set_skater_assignment(&mut self, object_slot: ObjectSlot, team: Team) {
        self.object = Some((object_slot, team));
    }

    pub(crate) fn clear_skater_assignment(&mut self) {
        self.object = None;
    }

    pub(crate) fn set_skater_team(&mut self, team: Team) {
        if let Some((_, current_team)) = &mut self.object {
            *current_team = team;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reused_player_slot_invalidates_the_previous_player_id() {
        let mut slots = PlayerSlots::new();
        let index = PlayerIndex(0);
        let previous_id = slots.insert(index, ServerPlayer::new_bot("previous"));

        assert!(slots.remove(previous_id).is_some());

        let current_id = slots.insert(index, ServerPlayer::new_bot("current"));
        assert_ne!(previous_id, current_id);
        assert!(slots.get_player(previous_id).is_none());
        assert!(slots.get_player(current_id).is_some());
    }
}

#[derive(Copy, Clone)]
pub(crate) struct PingData {
    pub min: f32,
    pub max: f32,
    pub avg: f32,
    pub deviation: f32,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum MuteStatus {
    NotMuted,
    ShadowMuted,
    Muted,
}
