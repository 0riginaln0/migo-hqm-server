use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};

use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arraydeque::{ArrayDeque, Wrapping};
use async_stream::stream;
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, Utc};
use futures::StreamExt;

use glam::Vec3;
use glamx::Rot3;
use std::error::Error;
use tokio::net::UdpSocket;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::gamemode::{ExitReason, GameMode};

use crate::ban::{BanCheck, BanCheckResponse};
use crate::game::{
    PhysicsConfiguration, PlayerId, PlayerIndex, PlayerInput, Puck, Rink, RulesState,
    ScoreboardValues, SkaterHand, SkaterObject, Team,
};
use crate::players::NetworkPlayerData;
pub(crate) use crate::players::{
    HQMMessage, MuteStatus, PlayerListExt, ServerPlayer, ServerPlayerData, ServerPlayersAndMessages,
};
use crate::protocol::{
    HQMClientToServerMessage, HQMMessageCodec, HQMMessageWriter, ObjectPacket, write_message,
    write_objects,
};
use crate::record::RecordingSaveMethod;
use crate::{ReplayRecording, ServerConfiguration};

pub(crate) const GAME_HEADER: &[u8] = b"Hock";

const UPDATE_PACKET_TYPE: u8 = 5;
const NEW_GAME_PACKET_TYPE: u8 = 6;
const MAX_MESSAGES_PER_UPDATE: usize = 15;

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum HQMClientVersion {
    Vanilla,
    Ping,
    PingRules,
}

impl HQMClientVersion {
    pub(crate) fn has_ping(self) -> bool {
        match self {
            HQMClientVersion::Vanilla => false,
            HQMClientVersion::Ping => true,
            HQMClientVersion::PingRules => true,
        }
    }

    pub(crate) fn has_rules(self) -> bool {
        match self {
            HQMClientVersion::Vanilla => false,
            HQMClientVersion::Ping => false,
            HQMClientVersion::PingRules => true,
        }
    }
}

#[derive(Debug, Clone)]
pub enum GameObject {
    Skater(PlayerId, SkaterObject),
    Puck(Puck),
}

pub(crate) trait ObjectExt {
    fn spawn_puck(&mut self, puck: Puck) -> Option<usize>;
    fn spawn_skater(&mut self, player_id: PlayerId, skater: SkaterObject) -> Option<usize>;
}

impl ObjectExt for [Option<GameObject>] {
    fn spawn_puck(&mut self, puck: Puck) -> Option<usize> {
        if let Some(object_index) = self.iter().position(|x| x.is_none()) {
            self[object_index] = Some(GameObject::Puck(puck));
            Some(object_index)
        } else {
            None
        }
    }

    fn spawn_skater(&mut self, player_id: PlayerId, skater: SkaterObject) -> Option<usize> {
        if let Some(object_index) = self.iter().position(|x| x.is_none()) {
            self[object_index] = Some(GameObject::Skater(player_id, skater));
            Some(object_index)
        } else {
            None
        }
    }
}

pub struct HQMTickHistory {
    pub(crate) game_step: u32,
    replay_queue: VecDeque<(Option<PlayerId>, ReplayTick)>,
    saved_history: VecDeque<ReplayTick>,

    pub(crate) history_length: usize,
}

impl HQMTickHistory {
    fn new() -> Self {
        Self {
            game_step: u32::MAX,
            replay_queue: Default::default(),
            saved_history: Default::default(),
            history_length: 0,
        }
    }

    fn clear(&mut self) {
        self.replay_queue.clear();
        self.saved_history.clear();
        self.game_step = u32::MAX;
    }

    pub fn is_in_replay(&self) -> bool {
        !self.replay_queue.is_empty()
    }

    pub fn add_replay_to_queue(
        &mut self,
        start_step: u32,
        end_step: u32,
        force_view: Option<PlayerId>,
    ) {
        if start_step > end_step {
            warn!("start_step must be less than or equal to end_step");
            return;
        }

        let game_step = self.game_step;

        let i_end = game_step.saturating_sub(end_step) as usize;
        let i_start = game_step.saturating_sub(start_step) as usize;

        let data = self
            .saved_history
            .range(i_end..=i_start)
            .rev()
            .map(|x| (force_view, x.clone()));
        self.replay_queue.extend(data);
    }

    fn check_replay(&mut self) -> Option<(Option<PlayerId>, ReplayTick)> {
        self.replay_queue.pop_front()
    }
}

struct ReplicationState {
    last_scoreboard: Option<ScoreboardValues>,
    packet: u32,
    saved_packets: Box<ArrayDeque<[ObjectPacket; 32], 192, Wrapping>>,
    saved_pings: Box<ArrayDeque<Instant, 100, Wrapping>>,
}

impl ReplicationState {
    fn new() -> Self {
        Self {
            last_scoreboard: None,
            packet: u32::MAX,
            saved_packets: Box::new(ArrayDeque::new()),
            saved_pings: Box::new(ArrayDeque::new()),
        }
    }

    fn new_game(&mut self) {
        self.last_scoreboard = None;
        self.packet = u32::MAX;
        self.saved_packets.clear();
        self.saved_pings.clear();
    }
}

struct RecordingState {
    data: BytesMut,
    message_pos: usize,
    last_packet: u32,
}

impl RecordingState {
    fn new() -> Self {
        Self {
            data: BytesMut::with_capacity(64 * 1024 * 1024),
            message_pos: 0,
            last_packet: u32::MAX,
        }
    }

    fn new_game(&mut self) {
        self.data.clear();
        self.message_pos = 0;
        self.last_packet = u32::MAX;
    }

    fn take_data(&mut self) -> BytesMut {
        std::mem::replace(&mut self.data, BytesMut::new())
    }
}

pub(crate) struct HQMServerState {
    pub(crate) player_message_state: ServerPlayersAndMessages,

    pub(crate) objects: Vec<Option<GameObject>>,

    pub(crate) replay: HQMTickHistory,
    replication: ReplicationState,
    recording: RecordingState,
}

impl HQMServerState {
    pub(crate) fn new() -> Self {
        Self {
            player_message_state: ServerPlayersAndMessages::new(),
            objects: vec![None; 32],
            replay: HQMTickHistory::new(),
            replication: ReplicationState::new(),
            recording: RecordingState::new(),
        }
    }

    fn new_game(&mut self) {
        self.player_message_state.new_game();

        self.replay.clear();
        self.replication.new_game();
        self.recording.new_game();

        self.objects = vec![None; 32];
    }

    pub fn set_hand(&mut self, hand: SkaterHand, player_id: PlayerId) {
        if let Some(player) = self.player_message_state.players.get_player_mut(player_id) {
            player.preferred_hand = hand;
            if let Some((object_index, _)) = player.object {
                if let Some(GameObject::Skater(_, ref mut skater)) = self.objects[object_index] {
                    skater.hand = hand;
                }
            }
        }
    }
    pub(crate) fn move_to_spectator(&mut self, player_id: PlayerId) -> bool {
        if let Some(player) = self.player_message_state.players.get_player_mut(player_id) {
            if let Some((object_index, _)) = player.object {
                player.object = None;
                self.objects[object_index] = None;
                let update = player.get_update_message(player_id.index);
                self.player_message_state
                    .add_global_message(update, true, true);
                return true;
            }
        }
        false
    }

    pub(crate) fn spawn_skater(
        &mut self,
        player_id: PlayerId,
        team: Team,
        pos: Vec3,
        rot: Rot3,
        keep_stick_position: bool,
    ) -> bool {
        if let Some(player) = self.player_message_state.players.get_player_mut(player_id) {
            if let Some((object_index, ref mut team2)) = player.object {
                if let Some(GameObject::Skater(_, ref mut skater)) = self.objects[object_index] {
                    let mut new_skater = SkaterObject::new(pos, rot, player.preferred_hand);
                    if keep_stick_position {
                        let stick_pos_diff = skater.stick_pos - skater.body.pos;
                        let frame_change = rot * skater.body.rot.inverse();
                        new_skater.stick_pos = pos + frame_change * stick_pos_diff;
                        new_skater.stick_rot = frame_change * skater.stick_rot;
                        new_skater.stick_placement = skater.stick_placement;
                        new_skater.stick_placement_delta = skater.stick_placement_delta;
                    }
                    *skater = new_skater;
                    *team2 = team;
                    let update = player.get_update_message(player_id.index);
                    self.player_message_state
                        .add_global_message(update, true, true);
                    return true;
                } else {
                    unreachable!();
                }
            } else {
                let hand = player.preferred_hand;

                let skater = SkaterObject::new(pos, rot, hand);
                if let Some(object_index) = self.objects.spawn_skater(player_id, skater) {
                    player.object = Some((object_index, team));
                    if let ServerPlayerData::NetworkPlayer { data } = &mut player.data {
                        data.view_player_index = player_id.index;
                    }
                    let update = player.get_update_message(player_id.index);
                    self.player_message_state
                        .add_global_message(update, true, true);
                    return true;
                }
            }
        }
        false
    }

    pub fn remove_player(&mut self, player_id: PlayerId, on_recording: bool) -> bool {
        if let Some(player) = self.player_message_state.players.get_player(player_id) {
            let update = HQMMessage::PlayerUpdate {
                player_index: player_id.index,
                data: None,
            };
            let object_index = player.object.map(|x| x.0);

            self.player_message_state.players[player_id.index.0].0 += 1;
            self.player_message_state.players[player_id.index.0].1 = None;
            if let Some(object_index) = object_index {
                self.objects[object_index] = None;
            }

            self.player_message_state
                .add_global_message(update, true, on_recording);

            true
        } else {
            false
        }
    }
}

pub(crate) struct HQMServer {
    pub(crate) state: HQMServerState,

    pub config: ServerConfiguration,

    pub physics_config: PhysicsConfiguration,
    pub rink: Rink,

    game_id: u32,
    pub is_muted: bool,
    pub start_time: DateTime<Utc>,

    has_current_game_been_active: bool,

    pub(crate) ban: Box<dyn BanCheck>,
    pub(crate) save_recording: Box<dyn RecordingSaveMethod>,
}

impl HQMServer {
    pub(crate) fn new(
        config: ServerConfiguration,
        physics_config: PhysicsConfiguration,
        ban: Box<dyn BanCheck>,
        save_recording: Box<dyn RecordingSaveMethod>,
    ) -> Self {
        HQMServer {
            state: HQMServerState::new(),
            physics_config,
            is_muted: false,
            config,
            game_id: 1,

            has_current_game_been_active: false,
            ban,
            save_recording,

            start_time: Default::default(),
            rink: Rink::new(30.0, 61.0, 8.5),
        }
    }

    pub(crate) async fn handle_message<B: GameMode>(
        &mut self,
        addr: SocketAddr,
        socket: &Arc<UdpSocket>,
        command: HQMClientToServerMessage,
        behaviour: &mut B,
        write_buf: &mut BytesMut,
    ) {
        match command {
            HQMClientToServerMessage::Join {
                version,
                player_name,
            } => {
                self.player_join(addr, version, player_name, behaviour);
            }
            HQMClientToServerMessage::Update {
                current_game_id,
                input,
                deltatime,
                new_known_packet,
                known_msg_pos,
                chat,
                version,
            } => self.player_update(
                addr,
                current_game_id,
                input,
                deltatime,
                new_known_packet,
                known_msg_pos,
                chat,
                version,
                behaviour,
            ),
            HQMClientToServerMessage::Exit => self.player_exit(addr, behaviour),
            HQMClientToServerMessage::ServerInfo { version, ping } => {
                self.request_info(socket, addr, version, ping, behaviour, write_buf)
                    .await;
            }
        }
    }

    async fn request_info<B: GameMode>(
        &self,
        socket: &Arc<UdpSocket>,
        addr: SocketAddr,
        _version: u32,
        ping: u32,
        behaviour: &B,
        write_buf: &mut BytesMut,
    ) {
        write_buf.clear();
        let mut writer = HQMMessageWriter::new(write_buf);
        writer.write_bytes_aligned(GAME_HEADER);
        writer.write_byte_aligned(1);
        writer.write_bits(8, 55);
        writer.write_u32_aligned(ping);

        let player_count = self.real_player_count();
        writer.write_bits(8, player_count as u32);
        writer.write_bits(4, 4);
        writer.write_bits(4, behaviour.server_list_team_size());

        writer.write_bytes_aligned_padded(32, self.config.server_name.as_ref());

        let socket = socket.clone();

        let slice: &[u8] = write_buf;
        let _ = socket.send_to(slice, addr).await;
    }

    fn real_player_count(&self) -> usize {
        let mut player_count = 0;
        for (_, player) in self.state.player_message_state.players.iter_players() {
            let is_actual_player = match player.data {
                ServerPlayerData::NetworkPlayer { .. } => true,
                ServerPlayerData::Bot { .. } => false,
            };
            if is_actual_player {
                player_count += 1;
            }
        }
        player_count
    }

    fn player_update<B: GameMode>(
        &mut self,
        addr: SocketAddr,
        current_game_id: u32,
        input: PlayerInput,
        deltatime: Option<u32>,
        new_known_packet: u32,
        known_msgpos: usize,
        chat: Option<(u8, String)>,
        client_version: HQMClientVersion,
        behaviour: &mut B,
    ) {
        let (player_id, player) = match self
            .state
            .player_message_state
            .players
            .find_player_by_addr_mut(addr)
        {
            Some(x) => x,
            None => {
                return;
            }
        };
        if let ServerPlayerData::NetworkPlayer { data } = &mut player.data {
            let time_received = Instant::now();

            let duration_since_packet =
                if data.game_id == current_game_id && data.known_packet < new_known_packet {
                    let ticks = &self.state.replication.saved_pings;
                    self.state
                        .replication
                        .packet
                        .checked_sub(new_known_packet)
                        .and_then(|diff| ticks.get(diff as usize))
                        .and_then(|last_time_received| {
                            time_received.checked_duration_since(*last_time_received)
                        })
                } else {
                    None
                };

            if let Some(duration_since_packet) = duration_since_packet {
                data.last_ping
                    .push_front(duration_since_packet.as_secs_f32());
            }

            data.inactivity = 0;
            data.client_version = client_version;
            data.known_packet = new_known_packet;
            player.input = input;
            data.game_id = current_game_id;
            data.known_msgpos = known_msgpos;

            if let Some(deltatime) = deltatime {
                data.deltatime = deltatime;
            }

            if let Some((rep, message)) = chat {
                if data.chat_rep != Some(rep) {
                    data.chat_rep = Some(rep);
                    self.process_message(message, player_id, behaviour);
                }
            }
        }
    }

    fn player_join<B: GameMode>(
        &mut self,
        addr: SocketAddr,
        player_version: u32,
        name: String,
        behaviour: &mut B,
    ) {
        let player_count = self.real_player_count();
        let max_player_count = self.config.player_max;
        if player_count >= max_player_count {
            return; // Ignore join request
        }
        if player_version != 55 {
            return; // Not the right version
        }
        let current_slot = self
            .state
            .player_message_state
            .players
            .find_player_by_addr(addr);
        if current_slot.is_some() {
            return; // Player has already joined
        }

        // Check ban list
        if self.ban.check_ip_banned(addr.ip()) != BanCheckResponse::Allowed {
            return;
        }

        if let Some(player_index) = self.add_player(&name, addr) {
            behaviour.after_player_join(self.into(), player_index);
            info!(
                "{} ({}) joined server from address {:?}",
                name, player_index, addr
            );
            let msg = format!("{name} joined");
            self.state.player_message_state.add_server_chat_message(msg);
        }
    }

    fn process_command<B: GameMode>(
        &mut self,
        command: &str,
        arg: &str,
        player_id: PlayerId,
        behaviour: &mut B,
    ) {
        match parse_chat_command(command, arg) {
            ChatCommand::Builtin(BuiltinCommand::Mute(Some(player_index))) => {
                self.mute_player(player_id, player_index);
            }
            ChatCommand::Builtin(BuiltinCommand::Unmute(Some(player_index))) => {
                self.unmute_player(player_id, player_index);
            }
            ChatCommand::Builtin(BuiltinCommand::MuteChat) => self.mute_chat(player_id),
            ChatCommand::Builtin(BuiltinCommand::UnmuteChat) => self.unmute_chat(player_id),
            ChatCommand::Builtin(BuiltinCommand::Kick(Some(player_index))) => {
                self.kick_player(player_id, player_index, false, behaviour);
            }
            ChatCommand::Builtin(BuiltinCommand::KickAll(name)) => {
                self.kick_all_matching(player_id, name, false, behaviour);
            }
            ChatCommand::Builtin(BuiltinCommand::Ban(Some(player_index))) => {
                self.kick_player(player_id, player_index, true, behaviour);
            }
            ChatCommand::Builtin(BuiltinCommand::BanAll(name)) => {
                self.kick_all_matching(player_id, name, true, behaviour);
            }
            ChatCommand::Builtin(BuiltinCommand::ClearBans) => self.clear_bans(player_id),
            ChatCommand::Builtin(BuiltinCommand::SetRecording(rule)) => {
                self.set_recording(player_id, rule);
            }
            ChatCommand::Builtin(BuiltinCommand::Lefty) => {
                self.state.set_hand(SkaterHand::Left, player_id);
            }
            ChatCommand::Builtin(BuiltinCommand::Righty) => {
                self.state.set_hand(SkaterHand::Right, player_id);
            }
            ChatCommand::Builtin(BuiltinCommand::Admin(password)) => {
                self.admin_login(player_id, password);
            }
            ChatCommand::Builtin(BuiltinCommand::RestartServer) => self.restart_server(player_id),
            ChatCommand::Builtin(BuiltinCommand::List(Some(first_index))) => {
                self.list_players(player_id, first_index);
            }
            ChatCommand::Builtin(BuiltinCommand::Search(name)) => {
                self.search_players(player_id, name)
            }
            ChatCommand::Builtin(BuiltinCommand::Ping(Some(player_index))) => {
                self.ping(player_index, player_id);
            }
            ChatCommand::Builtin(BuiltinCommand::PingByName(name)) => {
                self.ping_by_name(player_id, name);
            }
            ChatCommand::Builtin(BuiltinCommand::View(Some(player_index))) => {
                self.view(player_index, player_id);
            }
            ChatCommand::Builtin(BuiltinCommand::ViewByName(name)) => {
                self.view_by_name(player_id, name);
            }
            ChatCommand::Builtin(BuiltinCommand::RestoreView) => {
                if let Some(player) = self
                    .state
                    .player_message_state
                    .players
                    .get_player_mut(player_id)
                {
                    if let ServerPlayerData::NetworkPlayer { data } = &mut player.data {
                        if data.view_player_index != player_id.index {
                            data.view_player_index = player_id.index;
                            self.state
                                .player_message_state
                                .add_directed_server_chat_message(
                                    "View has been restored",
                                    player_id,
                                );
                        }
                    }
                }
            }
            ChatCommand::Builtin(BuiltinCommand::TeamChat(message)) => {
                self.state
                    .player_message_state
                    .add_user_team_message(message, player_id);
            }
            ChatCommand::Builtin(BuiltinCommand::Version) => {
                let version = env!("CARGO_PKG_VERSION");
                let s = format!("Migo HQM Server, version {version}");

                self.state
                    .player_message_state
                    .add_directed_server_chat_message(s, player_id);
            }
            ChatCommand::Builtin(BuiltinCommand::Git) => {
                let git_sha = option_env!("VERGEN_GIT_SHA");
                let s: Cow<'static, str> = if let Some(git_sha) = git_sha {
                    format!("Git commit: {git_sha}").into()
                } else {
                    "No git commit ID found".into()
                };
                self.state
                    .player_message_state
                    .add_directed_server_chat_message(s, player_id);
            }
            ChatCommand::Builtin(_) => {}
            ChatCommand::GameMode { command, arg } => {
                behaviour.handle_command(self.into(), command, arg, player_id);
            }
        }
    }

    fn list_players(&mut self, receiver_id: PlayerId, first_index: usize) {
        let res: Vec<_> = self
            .state
            .player_message_state
            .players
            .iter_players()
            .filter(|(x, _)| x.index.0 >= first_index)
            .take(5)
            .map(|(player_index, player)| format!("{}: {}", player_index.index, player.player_name))
            .collect();
        for msg in res {
            self.state
                .player_message_state
                .add_directed_server_chat_message(msg, receiver_id);
        }
    }

    fn search_players(&mut self, player_id: PlayerId, name: &str) {
        let matches = self.player_search(name);
        if matches.is_empty() {
            self.state
                .player_message_state
                .add_directed_server_chat_message("No matches found", player_id);
            return;
        }
        for (found_player_id, found_player_name) in matches.into_iter().take(5) {
            let msg = format!("{}: {}", found_player_id.index, found_player_name);
            self.state
                .player_message_state
                .add_directed_server_chat_message(msg, player_id);
        }
    }

    fn view(&mut self, view_player_index: PlayerIndex, player_id: PlayerId) {
        if let Some((view_player_id, view_player)) = self
            .state
            .player_message_state
            .players
            .get_player_by_index(view_player_index)
        {
            let view_player_name = view_player.player_name.clone();

            if let Some(player) = self
                .state
                .player_message_state
                .players
                .get_player_mut(player_id)
            {
                if let ServerPlayerData::NetworkPlayer { data } = &mut player.data {
                    if player.object.is_some() {
                        self.state
                            .player_message_state
                            .add_directed_server_chat_message(
                                "You must be a spectator to change view",
                                player_id,
                            );
                    } else if view_player_index != data.view_player_index {
                        data.view_player_index = view_player_id.index;
                        if player_id != view_player_id {
                            let msg = format!("You are now viewing {view_player_name}");
                            self.state
                                .player_message_state
                                .add_directed_server_chat_message(msg, player_id);
                        } else {
                            self.state
                                .player_message_state
                                .add_directed_server_chat_message(
                                    "View has been restored",
                                    player_id,
                                );
                        }
                    }
                }
            }
        } else {
            self.state
                .player_message_state
                .add_directed_server_chat_message("No player with this ID exists", player_id);
        }
    }

    fn ping(&mut self, ping_player_index: PlayerIndex, player_id: PlayerId) {
        if let Some((_, ping_player)) = self
            .state
            .player_message_state
            .players
            .get_player_by_index(ping_player_index)
        {
            if let Some(ping) = ping_player.ping_data() {
                let msg1 = format!(
                    "{} ping: avg {:.0} ms",
                    ping_player.player_name,
                    (ping.avg * 1000f32)
                );
                let msg2 = format!(
                    "min {:.0} ms, max {:.0} ms, std.dev {:.1}",
                    (ping.min * 1000f32),
                    (ping.max * 1000f32),
                    (ping.deviation * 1000f32)
                );
                self.state
                    .player_message_state
                    .add_directed_server_chat_message(msg1, player_id);
                self.state
                    .player_message_state
                    .add_directed_server_chat_message(msg2, player_id);
            } else {
                self.state
                    .player_message_state
                    .add_directed_server_chat_message(
                        "This player is not a connected player",
                        player_id,
                    );
            }
        } else {
            self.state
                .player_message_state
                .add_directed_server_chat_message("No player with this ID exists", player_id);
        }
    }

    pub fn player_exact_unique_match(&self, name: &str) -> Option<(PlayerId, Rc<str>)> {
        let mut found = None;
        for (player_id, player) in self.state.player_message_state.players.iter_players() {
            if player.player_name.as_ref() == name {
                if found.is_none() {
                    found = Some((player_id, player.player_name.clone()));
                } else {
                    return None;
                }
            }
        }
        found
    }

    pub fn player_search(&self, name: &str) -> Vec<(PlayerId, Rc<str>)> {
        let name = name.to_lowercase();
        let mut found = Vec::new();
        for (player_index, player) in self.state.player_message_state.players.iter_players() {
            if player.player_name.to_lowercase().contains(&name) {
                found.push((player_index, player.player_name.clone()));
                if found.len() >= 5 {
                    break;
                }
            }
        }
        found
    }

    fn player_index_from_name(
        &mut self,
        receiver_id: PlayerId,
        name: &str,
        command: &str,
    ) -> Option<PlayerIndex> {
        if let Some((player_id, _)) = self.player_exact_unique_match(name) {
            return Some(player_id.index);
        }

        let matches = self.player_search(name);
        match matches.as_slice() {
            [] => {
                self.state
                    .player_message_state
                    .add_directed_server_chat_message("No matches found", receiver_id);
                None
            }
            [(player_id, _)] => Some(player_id.index),
            _ => {
                let message = format!("Multiple matches found, use /{command} X");
                self.state
                    .player_message_state
                    .add_directed_server_chat_message(message, receiver_id);
                for (player_id, player_name) in matches {
                    let message = format!("{}: {}", player_id.index, player_name);
                    self.state
                        .player_message_state
                        .add_directed_server_chat_message(message, receiver_id);
                }
                None
            }
        }
    }

    fn ping_by_name(&mut self, player_id: PlayerId, name: &str) {
        if let Some(ping_player_index) = self.player_index_from_name(player_id, name, "ping") {
            self.ping(ping_player_index, player_id);
        }
    }

    fn view_by_name(&mut self, player_id: PlayerId, name: &str) {
        if let Some(view_player_index) = self.player_index_from_name(player_id, name, "view") {
            self.view(view_player_index, player_id);
        }
    }

    fn process_message<B: GameMode>(
        &mut self,
        msg: String,
        player_id: PlayerId,
        behaviour: &mut B,
    ) {
        if let Some(player) = self
            .state
            .player_message_state
            .players
            .get_player(player_id)
        {
            if msg.starts_with("/") {
                let split: Vec<&str> = msg.splitn(2, " ").collect();
                let command = &split[0][1..];
                let arg = if split.len() < 2 { "" } else { split[1] };
                self.process_command(command, arg, player_id, behaviour);
            } else if !self.is_muted {
                match player.is_muted {
                    MuteStatus::NotMuted => {
                        info!("{} ({}): {}", &player.player_name, player_id, &msg);
                        self.state
                            .player_message_state
                            .add_user_chat_message(msg, player_id.index);
                    }
                    MuteStatus::ShadowMuted => {
                        self.state
                            .player_message_state
                            .add_directed_user_chat_message(msg, player_id, player_id.index);
                    }
                    MuteStatus::Muted => {}
                }
            }
        }
    }

    fn player_exit<B: GameMode>(&mut self, addr: SocketAddr, behaviour: &mut B) {
        let player_id = self
            .state
            .player_message_state
            .players
            .find_player_by_addr(addr)
            .map(|(player_id, _)| player_id);

        if let Some(player_id) = player_id
            && let Some(player_name) =
                self.disconnect_player(player_id, ExitReason::Disconnected, behaviour)
        {
            info!("{} ({}) exited server", player_name, player_id);
            let msg = format!("{player_name} exited");
            self.state.player_message_state.add_server_chat_message(msg);
        }
    }

    fn disconnect_player<B: GameMode>(
        &mut self,
        player_id: PlayerId,
        reason: ExitReason,
        behaviour: &mut B,
    ) -> Option<Rc<str>> {
        let player_name = self
            .state
            .player_message_state
            .players
            .get_player(player_id)?
            .player_name
            .clone();
        behaviour.before_player_exit(self.into(), player_id, reason);
        self.state
            .remove_player(player_id, true)
            .then_some(player_name)
    }

    fn add_player(&mut self, player_name: &str, addr: SocketAddr) -> Option<PlayerId> {
        let res = self
            .state
            .player_message_state
            .add_player(player_name, addr);
        if let Some(player_index) = res {
            let welcome = self.config.welcome.clone();
            for welcome_msg in welcome {
                self.state
                    .player_message_state
                    .add_directed_server_chat_message(welcome_msg, player_index);
            }
        }
        res
    }

    fn game_step<B: GameMode>(&mut self, behaviour: &mut B) -> ScoreboardValues {
        self.state.replay.game_step = self.state.replay.game_step.wrapping_add(1);

        let events = self.simulate_step();

        let packets = self.get_packets();

        let scoreboard = behaviour.after_tick(self.into(), &events);
        self.state.replication.last_scoreboard = Some(scoreboard);

        if self.state.replay.history_length > 0 {
            let new_replay_tick = ReplayTick {
                game_step: self.state.replay.game_step,
                packets: packets.clone(),
            };
            self.state
                .replay
                .saved_history
                .truncate(self.state.replay.history_length - 1);
            self.state.replay.saved_history.push_front(new_replay_tick);
        } else {
            self.state.replay.saved_history.clear();
        }

        self.state.replication.saved_packets.push_front(packets);
        self.state.replication.packet = self.state.replication.packet.wrapping_add(1);

        if self.config.recording_enabled != ReplayRecording::Off
            && behaviour.include_tick_in_recording((&*self).into())
        {
            self.write_recording_tick(&scoreboard);
        }
        scoreboard
    }

    fn get_packets(&self) -> [ObjectPacket; 32] {
        let mut packets = [const { ObjectPacket::None }; 32];
        for (i, object) in self.state.objects.iter().enumerate() {
            packets[i] = match object {
                None => ObjectPacket::None,
                Some(GameObject::Skater(_, skater)) => ObjectPacket::Skater(skater.get_packet()),
                Some(GameObject::Puck(puck)) => ObjectPacket::Puck(puck.get_packet()),
            };
        }

        packets
    }

    fn remove_inactive_players<B: GameMode>(&mut self, behaviour: &mut B) {
        let inactive_players: smallvec::SmallVec<[_; 8]> = self
            .state
            .player_message_state
            .players
            .iter_players_mut()
            .filter_map(|(player_id, player)| {
                if let ServerPlayerData::NetworkPlayer { data } = &mut player.data {
                    data.inactivity += 1;
                    if data.inactivity > 500 {
                        Some(player_id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        for player_id in inactive_players {
            let Some(player_name) =
                self.disconnect_player(player_id, ExitReason::Timeout, behaviour)
            else {
                continue;
            };
            info!("{} ({}) timed out", player_name, player_id);
            let chat_msg = format!("{player_name} timed out");
            self.state
                .player_message_state
                .add_server_chat_message(chat_msg);
        }
    }

    fn advance_tick<B: GameMode>(&mut self, behaviour: &mut B) -> Option<TickOutput> {
        if self.real_player_count() == 0 {
            if self.has_current_game_been_active {
                info!("Game {} abandoned", self.game_id);
                self.new_game();
            }
            return None;
        }

        if !self.has_current_game_been_active {
            self.start_time = Utc::now();
            self.has_current_game_been_active = true;
            behaviour.game_started(self.into());
            info!("New game {} started", self.game_id);
        }

        self.remove_inactive_players(behaviour);
        behaviour.before_tick(self.into());

        let (game_step, forced_view, scoreboard) =
            if let Some((forced_view, tick)) = self.state.replay.check_replay() {
                let scoreboard = self.state.replication.last_scoreboard.unwrap_or_default();
                self.state
                    .replication
                    .saved_packets
                    .push_front(tick.packets);
                self.state.replication.packet = self.state.replication.packet.wrapping_add(1);
                (
                    tick.game_step,
                    forced_view.map(|player_id| player_id.index),
                    scoreboard,
                )
            } else {
                let scoreboard = self.game_step(behaviour);
                (self.state.replay.game_step, None, scoreboard)
            };

        self.state
            .replication
            .saved_pings
            .push_front(Instant::now());
        Some(TickOutput {
            game_step,
            forced_view,
            scoreboard,
        })
    }

    pub(crate) async fn tick<B: GameMode>(
        &mut self,
        socket: &UdpSocket,
        behaviour: &mut B,
        write_buf: &mut BytesMut,
    ) {
        let tick_output = tokio::task::block_in_place(|| self.advance_tick(behaviour));
        if let Some(tick_output) = tick_output {
            send_updates(
                self.game_id,
                &self.state.replication.saved_packets,
                tick_output.game_step,
                &tick_output.scoreboard,
                self.state.replication.packet,
                &self.state.player_message_state.players,
                socket,
                tick_output.forced_view,
                write_buf,
            )
            .await;
        }
    }

    fn save_recording(&mut self, old_recording_data: &[u8]) {
        let size = old_recording_data.len();
        let mut recording_data = BytesMut::with_capacity(size + 8);
        recording_data.put_u32_le(0u32);
        recording_data.put_u32_le(size as u32);
        recording_data.put_slice(old_recording_data);
        let recording_data = recording_data.freeze();
        self.save_recording
            .save_recording_data(&self.config, recording_data, self.start_time);
    }
    pub fn new_game(&mut self) {
        self.game_id += 1;

        self.has_current_game_been_active = false;

        let old_recording_data = self.state.recording.take_data();

        if self.config.recording_enabled == ReplayRecording::On && !old_recording_data.is_empty() {
            self.save_recording(&old_recording_data);
        }

        self.state.new_game();
    }

    fn write_recording_tick(&mut self, scoreboard: &ScoreboardValues) {
        let messages_to_write =
            &self.state.player_message_state.recording_messages[self.state.recording.message_pos..];
        let remaining_messages = messages_to_write.len();
        self.state.recording.data.reserve(
            9 // Header, time, score, period, etc.
            + 8 // Position metadata
            + (32*30) // 32 objects that can be at most 30 bytes each
            + 4 // Message metadata
            + remaining_messages * 66, // Chat message can be up to 66 bytes each
        );
        let mut writer = HQMMessageWriter::new(&mut self.state.recording.data);

        writer.write_byte_aligned(5);
        writer.write_bits(
            1,
            match scoreboard.game_over {
                true => 1,
                false => 0,
            },
        );
        writer.write_bits(8, scoreboard.red_score);
        writer.write_bits(8, scoreboard.blue_score);
        writer.write_bits(16, scoreboard.time);

        writer.write_bits(16, scoreboard.goal_message_timer);
        writer.write_bits(8, scoreboard.period); // 8.1

        let packets = &self.state.replication.saved_packets;

        write_objects(
            &mut writer,
            packets,
            self.state.replication.packet,
            self.state.recording.last_packet,
        );
        self.state.recording.last_packet = self.state.replication.packet;

        writer.write_bits(16, remaining_messages as u32);
        writer.write_bits(16, self.state.recording.message_pos as u32);

        for message in messages_to_write {
            write_message(&mut writer, Rc::as_ref(message));
        }
        self.state.recording.message_pos = self.state.player_message_state.recording_messages.len();
        writer.recording_fix();
    }
}

#[derive(Clone, Copy, Debug)]
struct TickOutput {
    game_step: u32,
    forced_view: Option<PlayerIndex>,
    scoreboard: ScoreboardValues,
}

#[derive(Clone, Debug)]
struct ReplayTick {
    game_step: u32,
    packets: [ObjectPacket; 32],
}

#[derive(Debug, PartialEq, Eq)]
enum BuiltinCommand<'a> {
    Mute(Option<PlayerIndex>),
    Unmute(Option<PlayerIndex>),
    MuteChat,
    UnmuteChat,
    Kick(Option<PlayerIndex>),
    KickAll(&'a str),
    Ban(Option<PlayerIndex>),
    BanAll(&'a str),
    ClearBans,
    SetRecording(&'a str),
    Lefty,
    Righty,
    Admin(&'a str),
    RestartServer,
    List(Option<usize>),
    Search(&'a str),
    Ping(Option<PlayerIndex>),
    PingByName(&'a str),
    View(Option<PlayerIndex>),
    ViewByName(&'a str),
    RestoreView,
    TeamChat(&'a str),
    Version,
    Git,
}

#[derive(Debug, PartialEq, Eq)]
enum ChatCommand<'a> {
    Builtin(BuiltinCommand<'a>),
    GameMode { command: &'a str, arg: &'a str },
}

fn parse_chat_command<'a>(command: &'a str, arg: &'a str) -> ChatCommand<'a> {
    let builtin = match command {
        "mute" => BuiltinCommand::Mute(arg.parse().ok()),
        "unmute" => BuiltinCommand::Unmute(arg.parse().ok()),
        "mutechat" => BuiltinCommand::MuteChat,
        "unmutechat" => BuiltinCommand::UnmuteChat,
        "kick" => BuiltinCommand::Kick(arg.parse().ok()),
        "kickall" => BuiltinCommand::KickAll(arg),
        "ban" => BuiltinCommand::Ban(arg.parse().ok()),
        "banall" => BuiltinCommand::BanAll(arg),
        "clearbans" => BuiltinCommand::ClearBans,
        "replay" | "record" => BuiltinCommand::SetRecording(arg),
        "lefty" => BuiltinCommand::Lefty,
        "righty" => BuiltinCommand::Righty,
        "admin" => BuiltinCommand::Admin(arg),
        "serverrestart" => BuiltinCommand::RestartServer,
        "list" => BuiltinCommand::List(if arg.is_empty() {
            Some(0)
        } else {
            arg.parse().ok()
        }),
        "search" => BuiltinCommand::Search(arg),
        "ping" => BuiltinCommand::Ping(arg.parse().ok()),
        "pings" => BuiltinCommand::PingByName(arg),
        "view" => BuiltinCommand::View(arg.parse().ok()),
        "views" => BuiltinCommand::ViewByName(arg),
        "restoreview" => BuiltinCommand::RestoreView,
        "t" => BuiltinCommand::TeamChat(arg),
        "version" => BuiltinCommand::Version,
        "git" => BuiltinCommand::Git,
        _ => {
            return ChatCommand::GameMode { command, arg };
        }
    };
    ChatCommand::Builtin(builtin)
}

#[derive(Clone, Copy)]
struct UpdatePacket<'a> {
    game_id: u32,
    packets: &'a ArrayDeque<[ObjectPacket; 32], 192, Wrapping>,
    game_step: u32,
    scoreboard: &'a ScoreboardValues,
    current_packet: u32,
    force_view: Option<PlayerIndex>,
}

fn encode_update_packet(
    write_buf: &mut BytesMut,
    update: UpdatePacket<'_>,
    player: &NetworkPlayerData,
) {
    write_buf.clear();
    let mut writer = HQMMessageWriter::new(write_buf);

    if player.game_id != update.game_id {
        writer.write_bytes_aligned(GAME_HEADER);
        writer.write_byte_aligned(NEW_GAME_PACKET_TYPE);
        writer.write_u32_aligned(update.game_id);
        return;
    }

    writer.write_bytes_aligned(GAME_HEADER);
    writer.write_byte_aligned(UPDATE_PACKET_TYPE);
    writer.write_u32_aligned(update.game_id);
    writer.write_u32_aligned(update.game_step);
    writer.write_bits(1, u32::from(update.scoreboard.game_over));
    writer.write_bits(8, update.scoreboard.red_score);
    writer.write_bits(8, update.scoreboard.blue_score);
    writer.write_bits(16, update.scoreboard.time);

    writer.write_bits(16, update.scoreboard.goal_message_timer);
    writer.write_bits(8, update.scoreboard.period);
    let view = update.force_view.unwrap_or(player.view_player_index).0 as u32;
    writer.write_bits(8, view);

    if player.client_version.has_ping() {
        writer.write_u32_aligned(player.deltatime);
    }

    if player.client_version.has_rules() {
        let rules = match update.scoreboard.rules_state {
            RulesState::Regular {
                offside_warning,
                icing_warning,
            } => u32::from(offside_warning) | (u32::from(icing_warning) << 1),
            RulesState::Offside => 4,
            RulesState::Icing => 8,
        };
        writer.write_u32_aligned(rules);
    }

    write_objects(
        &mut writer,
        update.packets,
        update.current_packet,
        player.known_packet,
    );

    let start = player.known_msgpos.min(player.messages.len());
    let remaining_messages = (player.messages.len() - start).min(MAX_MESSAGES_PER_UPDATE);
    writer.write_bits(4, remaining_messages as u32);
    writer.write_bits(16, start as u32);

    for message in &player.messages[start..start + remaining_messages] {
        write_message(&mut writer, Rc::as_ref(message));
    }
}

async fn send_updates(
    game_id: u32,
    packets: &ArrayDeque<[ObjectPacket; 32], 192, Wrapping>,
    game_step: u32,
    scoreboard: &ScoreboardValues,
    current_packet: u32,
    players: &[(u32, Option<ServerPlayer>)],
    socket: &UdpSocket,
    force_view: Option<PlayerIndex>,
    write_buf: &mut BytesMut,
) {
    let update = UpdatePacket {
        game_id,
        packets,
        game_step,
        scoreboard,
        current_packet,
        force_view,
    };

    for (_, player) in players.iter_players() {
        if let ServerPlayerData::NetworkPlayer { data } = &player.data {
            encode_update_packet(write_buf, update, data);
            let slice: &[u8] = write_buf;
            let _ = socket.send_to(slice, data.addr).await;
        }
    }
}

/// Starts an HQM server. This method will not return until the server has terminated.
pub async fn run_server<B: GameMode>(
    port: u16,
    public: Option<&str>,
    config: ServerConfiguration,
    physics_config: PhysicsConfiguration,
    ban: Box<dyn BanCheck>,
    recording: Box<dyn RecordingSaveMethod>,
    mut behaviour: B,
) -> std::io::Result<()> {
    let reqwest_client = reqwest::Client::new();

    let mut server = HQMServer::new(config, physics_config, ban, recording);
    info!("Server started");

    behaviour.init((&mut server).into());

    // Set up timers
    let mut tick_timer = tokio::time::interval(Duration::from_millis(10));
    tick_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let socket = Arc::new(tokio::net::UdpSocket::bind(&addr).await?);
    info!(
        "Server listening at address {:?}",
        socket.local_addr().unwrap()
    );

    async fn get_http_response(
        client: &reqwest::Client,
        address: &str,
    ) -> Result<SocketAddr, Box<dyn Error + Send + Sync>> {
        let response = client.get(address).send().await?.text().await?;

        let split = response.split_ascii_whitespace().collect::<Vec<&str>>();

        let addr = split.get(1).unwrap_or(&"").parse::<IpAddr>()?;
        let port = split.get(2).unwrap_or(&"").parse::<u16>()?;
        Ok(SocketAddr::new(addr, port))
    }

    if let Some(public) = public {
        let socket = socket.clone();
        let reqwest_client = reqwest_client.clone();
        let address = public.to_string();
        tokio::spawn(async move {
            loop {
                let master_server = get_http_response(&reqwest_client, &address).await;
                match master_server {
                    Ok(addr) => {
                        for _ in 0..60 {
                            let msg = b"Hock\x20";
                            let res = socket.send_to(msg, addr).await;
                            if res.is_err() {
                                break;
                            }
                            tokio::time::sleep(Duration::from_secs(10)).await;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(e);
                        tokio::time::sleep(Duration::from_secs(15)).await;
                    }
                }
            }
        });
    }
    enum Msg {
        Time,
        Message(SocketAddr, HQMClientToServerMessage),
    }

    let timeout_stream = tokio_stream::wrappers::IntervalStream::new(tick_timer).map(|_| Msg::Time);
    let packet_stream = {
        let socket = socket.clone();
        stream! {
            let mut buf = BytesMut::with_capacity(512);
            let codec = HQMMessageCodec;
            loop {
                buf.clear();

                if let Ok((_, addr)) = socket.recv_buf_from(&mut buf).await {
                    if let Ok(data) = codec.parse_message(&buf) {
                        yield Msg::Message(addr, data)
                    }
                }
            }
        }
    };
    tokio::pin!(packet_stream);

    let mut stream = futures::stream_select!(timeout_stream, packet_stream);
    let mut write_buf = BytesMut::with_capacity(4096);
    while let Some(msg) = stream.next().await {
        match msg {
            Msg::Time => server.tick(&socket, &mut behaviour, &mut write_buf).await,
            Msg::Message(addr, data) => {
                server
                    .handle_message(addr, &socket, data, &mut behaviour, &mut write_buf)
                    .await
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::players::ServerPlayer;

    fn network_player() -> ServerPlayer {
        ServerPlayer::new_network_player(
            PlayerIndex(0),
            "test",
            "127.0.0.1:12345".parse().unwrap(),
            &[],
        )
    }

    #[test]
    fn parses_builtin_commands_without_forwarding_them_to_the_game_mode() {
        assert_eq!(
            parse_chat_command("kick", "12"),
            ChatCommand::Builtin(BuiltinCommand::Kick(Some(PlayerIndex(12))))
        );
        assert_eq!(
            parse_chat_command("record", "standby"),
            ChatCommand::Builtin(BuiltinCommand::SetRecording("standby"))
        );
        assert_eq!(
            parse_chat_command("list", ""),
            ChatCommand::Builtin(BuiltinCommand::List(Some(0)))
        );
        assert_eq!(
            parse_chat_command("view", "not-a-player"),
            ChatCommand::Builtin(BuiltinCommand::View(None))
        );
    }

    #[test]
    fn preserves_unknown_commands_for_the_game_mode() {
        match parse_chat_command("faceoff", "center") {
            ChatCommand::GameMode { command, arg } => {
                assert_eq!(command, "faceoff");
                assert_eq!(arg, "center");
            }
            ChatCommand::Builtin(_) => panic!("unknown command was treated as built-in"),
        }
    }

    #[test]
    fn encodes_new_game_packet_without_replication_data() {
        let mut player = network_player();
        let ServerPlayerData::NetworkPlayer { data } = &mut player.data else {
            panic!("expected a network player");
        };
        let mut buf = BytesMut::new();
        let packets = ArrayDeque::new();
        let scoreboard = ScoreboardValues::default();

        encode_update_packet(
            &mut buf,
            UpdatePacket {
                game_id: 42,
                packets: &packets,
                game_step: 0,
                scoreboard: &scoreboard,
                current_packet: 0,
                force_view: None,
            },
            data,
        );

        assert_eq!(buf.as_ref(), b"Hock\x06\x2a\x00\x00\x00");
    }

    #[test]
    fn encodes_update_packet_when_the_client_is_in_the_current_game() {
        let mut player = network_player();
        let ServerPlayerData::NetworkPlayer { data } = &mut player.data else {
            panic!("expected a network player");
        };
        data.game_id = 42;

        let mut packets: ArrayDeque<[ObjectPacket; 32], 192, Wrapping> = ArrayDeque::new();
        packets.push_front([const { ObjectPacket::None }; 32]);
        let scoreboard = ScoreboardValues::default();
        let mut buf = BytesMut::new();

        encode_update_packet(
            &mut buf,
            UpdatePacket {
                game_id: 42,
                packets: &packets,
                game_step: 11,
                scoreboard: &scoreboard,
                current_packet: 0,
                force_view: None,
            },
            data,
        );

        assert_eq!(&buf[..9], b"Hock\x05\x2a\x00\x00\x00");
        assert!(buf.len() > 9);
    }
}
