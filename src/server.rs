use std::borrow::Cow;
use std::cmp::min;
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

pub(crate) struct HQMServerState {
    pub(crate) player_message_state: ServerPlayersAndMessages,

    pub(crate) objects: Vec<Option<GameObject>>,

    pub(crate) replay: HQMTickHistory,

    last_scoreboard: Option<ScoreboardValues>,

    packet: u32,
    recording_data: BytesMut,
    recording_msg_pos: usize,
    recording_last_packet: u32,

    saved_packets: Box<ArrayDeque<[ObjectPacket; 32], 192, Wrapping>>,

    saved_pings: Box<ArrayDeque<Instant, 100, Wrapping>>,
}

impl HQMServerState {
    pub(crate) fn new() -> Self {
        Self {
            player_message_state: ServerPlayersAndMessages::new(),
            objects: vec![None; 32],
            replay: HQMTickHistory::new(),
            last_scoreboard: None,

            recording_data: BytesMut::with_capacity(64 * 1024 * 1024),
            recording_msg_pos: 0,
            packet: u32::MAX,
            recording_last_packet: u32::MAX,

            saved_packets: Box::new(ArrayDeque::new()),

            saved_pings: Box::new(ArrayDeque::new()),
        }
    }

    fn new_game(&mut self) {
        self.player_message_state.new_game();

        self.replay.clear();

        self.recording_msg_pos = 0;
        self.packet = u32::MAX;
        self.recording_last_packet = u32::MAX;

        self.saved_packets.clear();

        self.saved_pings.clear();

        self.objects = vec![None; 32];
        self.last_scoreboard = None;
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
                    let ticks = &self.state.saved_pings;
                    self.state
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
        match command {
            "mute" => {
                if let Ok(mute_player_index) = arg.parse::<PlayerIndex>() {
                    self.mute_player(player_id, mute_player_index);
                }
            }
            "unmute" => {
                if let Ok(mute_player_index) = arg.parse::<PlayerIndex>() {
                    self.unmute_player(player_id, mute_player_index);
                }
            }
            /*"shadowmute" => {
                if let Ok(mute_player_index) = arg.parse::<usize>() {
                    if mute_player_index < self.players.len() {
                        self.shadowmute_player(player_index, mute_player_index);
                    }
                }
            },*/
            "mutechat" => {
                self.mute_chat(player_id);
            }
            "unmutechat" => {
                self.unmute_chat(player_id);
            }
            "kick" => {
                if let Ok(kick_player_index) = arg.parse::<PlayerIndex>() {
                    self.kick_player(player_id, kick_player_index, false, behaviour);
                }
            }
            "kickall" => {
                self.kick_all_matching(player_id, arg, false, behaviour);
            }
            "ban" => {
                if let Ok(kick_player_index) = arg.parse::<PlayerIndex>() {
                    self.kick_player(player_id, kick_player_index, true, behaviour);
                }
            }
            "banall" => {
                self.kick_all_matching(player_id, arg, true, behaviour);
            }
            "clearbans" => {
                self.clear_bans(player_id);
            }
            "replay" | "record" => self.set_recording(player_id, arg),
            "lefty" => {
                self.state.set_hand(SkaterHand::Left, player_id);
            }
            "righty" => {
                self.state.set_hand(SkaterHand::Right, player_id);
            }
            "admin" => {
                self.admin_login(player_id, arg);
            }
            "serverrestart" => {
                self.restart_server(player_id);
            }
            "list" => {
                if arg.is_empty() {
                    self.list_players(player_id, 0);
                } else if let Ok(first_index) = arg.parse::<usize>() {
                    self.list_players(player_id, first_index);
                }
            }
            "search" => {
                self.search_players(player_id, arg);
            }
            "ping" => {
                if let Ok(ping_player_index) = arg.parse::<PlayerIndex>() {
                    self.ping(ping_player_index, player_id);
                }
            }
            "pings" => {
                if let Some((ping_player_id, _name)) = self.player_exact_unique_match(arg) {
                    self.ping(ping_player_id.index, player_id);
                } else {
                    let matches = self.player_search(arg);
                    if matches.is_empty() {
                        self.state
                            .player_message_state
                            .add_directed_server_chat_message("No matches found", player_id);
                    } else if matches.len() > 1 {
                        self.state
                            .player_message_state
                            .add_directed_server_chat_message(
                                "Multiple matches found, use /ping X",
                                player_id,
                            );
                        for (found_player_id, found_player_name) in matches.into_iter().take(5) {
                            let msg = format!("{}: {}", found_player_id.index, found_player_name);
                            self.state
                                .player_message_state
                                .add_directed_server_chat_message(msg, player_id);
                        }
                    } else {
                        self.ping(matches[0].0.index, player_id);
                    }
                }
            }
            "view" => {
                if let Ok(view_player_index) = arg.parse::<PlayerIndex>() {
                    self.view(view_player_index, player_id);
                }
            }
            "views" => {
                if let Some((view_player_id, _name)) = self.player_exact_unique_match(arg) {
                    self.view(view_player_id.index, player_id);
                } else {
                    let matches = self.player_search(arg);
                    if matches.is_empty() {
                        self.state
                            .player_message_state
                            .add_directed_server_chat_message("No matches found", player_id);
                    } else if matches.len() > 1 {
                        self.state
                            .player_message_state
                            .add_directed_server_chat_message(
                                "Multiple matches found, use /view X",
                                player_id,
                            );
                        for (found_player_id, found_player_name) in matches.into_iter().take(5) {
                            let str = format!("{}: {}", found_player_id.index, found_player_name);
                            self.state
                                .player_message_state
                                .add_directed_server_chat_message(str, player_id);
                        }
                    } else {
                        self.view(matches[0].0.index, player_id);
                    }
                }
            }
            "restoreview" => {
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
            "t" => {
                self.state
                    .player_message_state
                    .add_user_team_message(arg, player_id);
            }
            "version" => {
                let version = env!("CARGO_PKG_VERSION");
                let s = format!("Migo HQM Server, version {version}");

                self.state
                    .player_message_state
                    .add_directed_server_chat_message(s, player_id);
            }
            "git" => {
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

            _ => behaviour.handle_command(self.into(), command, arg, player_id),
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
        let player = self
            .state
            .player_message_state
            .players
            .find_player_by_addr(addr);

        if let Some((player_id, player)) = player {
            let player_name = player.player_name.clone();
            behaviour.before_player_exit(self.into(), player_id, ExitReason::Disconnected);
            self.state.remove_player(player_id, true);
            info!("{} ({}) exited server", player_name, player_id);
            let msg = format!("{player_name} exited");
            self.state.player_message_state.add_server_chat_message(msg);
        }
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
        self.state.last_scoreboard = Some(scoreboard);

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

        self.state.saved_packets.push_front(packets);
        self.state.packet = self.state.packet.wrapping_add(1);

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
                        Some((player_id, player.player_name.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        for (player_id, player_name) in inactive_players {
            behaviour.before_player_exit(self.into(), player_id, ExitReason::Timeout);
            self.state.remove_player(player_id, true);
            info!("{} ({}) timed out", player_name, player_id);
            let chat_msg = format!("{player_name} timed out");
            self.state
                .player_message_state
                .add_server_chat_message(chat_msg);
        }
    }

    pub(crate) async fn tick<B: GameMode>(
        &mut self,
        socket: &UdpSocket,
        behaviour: &mut B,
        write_buf: &mut BytesMut,
    ) {
        if self.real_player_count() != 0 {
            if !self.has_current_game_been_active {
                self.start_time = Utc::now();
                self.has_current_game_been_active = true;
                behaviour.game_started(self.into());
                info!("New game {} started", self.game_id);
            }

            let (game_step, forced_view, scoreboard) = tokio::task::block_in_place(|| {
                self.remove_inactive_players(behaviour);

                behaviour.before_tick(self.into());

                let has_replay_data = self.state.replay.check_replay();

                let res = if let Some((forced_view, tick)) = has_replay_data {
                    let forced_view = forced_view.map(|x| x.index);
                    let game_step = tick.game_step;
                    let packets = tick.packets;
                    let scoreboard = self.state.last_scoreboard.unwrap_or_default();

                    self.state.saved_packets.push_front(packets);

                    self.state.packet = self.state.packet.wrapping_add(1);
                    (game_step, forced_view, scoreboard)
                } else {
                    let scoreboard = self.game_step(behaviour);
                    (self.state.replay.game_step, None, scoreboard)
                };

                self.state.saved_pings.push_front(Instant::now());

                res
            });

            send_updates(
                self.game_id,
                &self.state.saved_packets,
                game_step,
                &scoreboard,
                self.state.packet,
                &self.state.player_message_state.players,
                socket,
                forced_view,
                write_buf,
            )
            .await;
        } else if self.has_current_game_been_active {
            info!("Game {} abandoned", self.game_id);
            self.new_game();
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

        let old_recording_data = std::mem::replace(&mut self.state.recording_data, BytesMut::new());

        if self.config.recording_enabled == ReplayRecording::On && !old_recording_data.is_empty() {
            self.save_recording(&old_recording_data);
        }

        self.state.new_game();
    }

    fn write_recording_tick(&mut self, scoreboard: &ScoreboardValues) {
        let messages_to_write =
            &self.state.player_message_state.recording_messages[self.state.recording_msg_pos..];
        let remaining_messages = messages_to_write.len();
        self.state.recording_data.reserve(
            9 // Header, time, score, period, etc.
            + 8 // Position metadata
            + (32*30) // 32 objects that can be at most 30 bytes each
            + 4 // Message metadata
            + remaining_messages * 66, // Chat message can be up to 66 bytes each
        );
        let mut writer = HQMMessageWriter::new(&mut self.state.recording_data);

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

        let packets = &self.state.saved_packets;

        write_objects(
            &mut writer,
            packets,
            self.state.packet,
            self.state.recording_last_packet,
        );
        self.state.recording_last_packet = self.state.packet;

        writer.write_bits(16, remaining_messages as u32);
        writer.write_bits(16, self.state.recording_msg_pos as u32);

        for message in messages_to_write {
            write_message(&mut writer, Rc::as_ref(message));
        }
        self.state.recording_msg_pos = self.state.player_message_state.recording_messages.len();
        writer.recording_fix();
    }
}

#[derive(Clone, Debug)]
struct ReplayTick {
    game_step: u32,
    packets: [ObjectPacket; 32],
}

async fn send_updates(
    game_id: u32,
    packets: &ArrayDeque<[ObjectPacket; 32], 192, Wrapping>,
    game_step: u32,
    value: &ScoreboardValues,
    current_packet: u32,
    players: &[(u32, Option<ServerPlayer>)],
    socket: &UdpSocket,
    force_view: Option<PlayerIndex>,
    write_buf: &mut BytesMut,
) {
    for (_, player) in players.iter_players() {
        if let ServerPlayerData::NetworkPlayer { data } = &player.data {
            write_buf.clear();
            let mut writer = HQMMessageWriter::new(write_buf);

            if data.game_id != game_id {
                writer.write_bytes_aligned(GAME_HEADER);
                writer.write_byte_aligned(6);
                writer.write_u32_aligned(game_id);
            } else {
                writer.write_bytes_aligned(GAME_HEADER);
                writer.write_byte_aligned(5);
                writer.write_u32_aligned(game_id);
                writer.write_u32_aligned(game_step);
                writer.write_bits(
                    1,
                    match value.game_over {
                        true => 1,
                        false => 0,
                    },
                );
                writer.write_bits(8, value.red_score);
                writer.write_bits(8, value.blue_score);
                writer.write_bits(16, value.time);

                writer.write_bits(16, value.goal_message_timer);
                writer.write_bits(8, value.period);
                let view = force_view.unwrap_or(data.view_player_index).0 as u32;
                writer.write_bits(8, view);

                // if using a non-cryptic version, send ping
                if data.client_version.has_ping() {
                    writer.write_u32_aligned(data.deltatime);
                }

                // if baba's second version or above, send rules
                if data.client_version.has_rules() {
                    let num = match value.rules_state {
                        RulesState::Regular {
                            offside_warning,
                            icing_warning,
                        } => {
                            let mut res = 0;
                            if offside_warning {
                                res |= 1;
                            }
                            if icing_warning {
                                res |= 2;
                            }
                            res
                        }
                        RulesState::Offside => 4,
                        RulesState::Icing => 8,
                    };
                    writer.write_u32_aligned(num);
                }

                write_objects(&mut writer, packets, current_packet, data.known_packet);

                let (start, remaining_messages) = if data.known_msgpos > data.messages.len() {
                    (data.messages.len(), 0)
                } else {
                    (
                        data.known_msgpos,
                        min(data.messages.len() - data.known_msgpos, 15),
                    )
                };

                writer.write_bits(4, remaining_messages as u32);
                writer.write_bits(16, start as u32);

                for message in &data.messages[start..start + remaining_messages] {
                    write_message(&mut writer, Rc::as_ref(message));
                }
            }

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
