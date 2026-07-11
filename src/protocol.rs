use crate::game::PlayerInput;
use crate::server::{HQMClientVersion, HQMMessage, PacketHistory, PacketNumber};
use bytes::{BufMut, BytesMut};

use glam::{Vec2, Vec3};
use glamx::Rot3;
use std::cmp::min;
use std::io::Error;
use std::string::FromUtf8Error;

const TABLE: [[Vec3; 3]; 8] = [
    [Vec3::Y, Vec3::X, Vec3::Z],
    [Vec3::Y, Vec3::Z, Vec3::NEG_X],
    [Vec3::Y, Vec3::NEG_Z, Vec3::X],
    [Vec3::Y, Vec3::NEG_X, Vec3::NEG_Z],
    [Vec3::Z, Vec3::X, Vec3::NEG_Y],
    [Vec3::NEG_X, Vec3::Z, Vec3::NEG_Y],
    [Vec3::X, Vec3::NEG_Z, Vec3::NEG_Y],
    [Vec3::NEG_Z, Vec3::NEG_X, Vec3::NEG_Y],
];

const GAME_HEADER: &[u8] = b"Hock";

pub enum HQMClientToServerMessage {
    Join {
        version: u32,
        player_name: String,
    },
    Update {
        current_game_id: u32,
        input: PlayerInput,
        deltatime: Option<u32>,
        new_known_packet: u32,
        known_msg_pos: usize,
        chat: Option<(u8, String)>,
        version: HQMClientVersion,
    },
    Exit,
    ServerInfo {
        version: u32,
        ping: u32,
    },
}

pub struct HQMMessageCodec;

impl HQMMessageCodec {
    pub fn parse_message(
        &self,
        src: &[u8],
    ) -> Result<HQMClientToServerMessage, HQMClientToServerMessageDecoderError> {
        let mut parser = HQMMessageReader::new(src);
        let mut header = [0; 4];
        parser.read_bytes_aligned(&mut header);
        if header != GAME_HEADER {
            return Err(HQMClientToServerMessageDecoderError::WrongHeader);
        }

        let command = parser.read_byte_aligned();
        match command {
            0 => self.parse_request_info(&mut parser),
            2 => self.parse_player_join(&mut parser),
            4 => self.parse_player_update(&mut parser, HQMClientVersion::Vanilla),
            8 => self.parse_player_update(&mut parser, HQMClientVersion::Ping),
            0x10 => self.parse_player_update(&mut parser, HQMClientVersion::PingRules),
            7 => Ok(HQMClientToServerMessage::Exit),
            _ => Err(HQMClientToServerMessageDecoderError::UnknownType),
        }
    }

    fn parse_request_info(
        &self,
        parser: &mut HQMMessageReader,
    ) -> Result<HQMClientToServerMessage, HQMClientToServerMessageDecoderError> {
        let version = parser.read_bits(8);
        let ping = parser.read_u32_aligned();
        Ok(HQMClientToServerMessage::ServerInfo { version, ping })
    }

    fn parse_player_join(
        &self,
        parser: &mut HQMMessageReader,
    ) -> Result<HQMClientToServerMessage, HQMClientToServerMessageDecoderError> {
        let version = parser.read_bits(8);
        let mut player_name = [0; 32];
        parser.read_bytes_aligned(&mut player_name);
        let player_name = get_player_name(&player_name)?;
        Ok(HQMClientToServerMessage::Join {
            version,
            player_name,
        })
    }

    fn parse_player_update(
        &self,
        parser: &mut HQMMessageReader,
        client_version: HQMClientVersion,
    ) -> Result<HQMClientToServerMessage, HQMClientToServerMessageDecoderError> {
        let current_game_id = parser.read_u32_aligned();

        let input_stick_angle = parser.read_f32_aligned();
        let input_turn = parser.read_f32_aligned();
        let _input_unknown = parser.read_f32_aligned();
        let input_fwbw = parser.read_f32_aligned();
        let input_stick_rot_1 = parser.read_f32_aligned();
        let input_stick_rot_2 = parser.read_f32_aligned();
        let input_head_rot = parser.read_f32_aligned();
        let input_body_rot = parser.read_f32_aligned();
        let input_keys = parser.read_u32_aligned();
        let input = PlayerInput {
            stick_angle: input_stick_angle,
            turn: input_turn,
            fwbw: input_fwbw,
            stick: Vec2::new(input_stick_rot_1, input_stick_rot_2),
            head_rot: input_head_rot,
            body_rot: input_body_rot,
            keys: input_keys,
        };

        let deltatime = if client_version.has_ping() {
            Some(parser.read_u32_aligned())
        } else {
            None
        };

        let new_known_packet = parser.read_u32_aligned();
        let known_msg_pos = parser.read_u16_aligned() as usize;

        let chat = {
            let has_chat_msg = parser.read_bits(1) == 1;
            if has_chat_msg {
                let rep = parser.read_bits(3) as u8;
                let byte_num = parser.read_bits(8) as usize;
                let mut bytes = [0; 256];

                parser.read_bytes_aligned(&mut bytes[0..byte_num]);
                let msg = String::from_utf8(bytes[0..byte_num].to_vec())?;
                Some((rep, msg))
            } else {
                None
            }
        };

        Ok(HQMClientToServerMessage::Update {
            current_game_id,
            input,
            deltatime,
            new_known_packet,
            known_msg_pos,
            chat,
            version: client_version,
        })
    }
}
pub enum HQMClientToServerMessageDecoderError {
    IoError(std::io::Error),
    WrongHeader,
    UnknownType,
    StringDecoding(FromUtf8Error),
}

impl From<std::io::Error> for HQMClientToServerMessageDecoderError {
    fn from(value: Error) -> Self {
        HQMClientToServerMessageDecoderError::IoError(value)
    }
}

impl From<FromUtf8Error> for HQMClientToServerMessageDecoderError {
    fn from(value: FromUtf8Error) -> Self {
        HQMClientToServerMessageDecoderError::StringDecoding(value)
    }
}

fn get_player_name(bytes: &[u8]) -> Result<String, FromUtf8Error> {
    let first_null = bytes.iter().position(|x| *x == 0);

    let bytes = match first_null {
        Some(x) => &bytes[0..x],
        None => bytes,
    }
    .to_vec();
    let name = String::from_utf8(bytes)?;
    Ok(if name.is_empty() {
        "Noname".to_owned()
    } else {
        name
    })
}

pub fn convert_matrix_to_network(b: u8, v: &Rot3) -> (u32, u32) {
    let r1 = convert_rot_column_to_network(b, v * Vec3::Y);
    let r2 = convert_rot_column_to_network(b, v * Vec3::Z);
    (r1, r2)
}

#[allow(dead_code)]
pub fn convert_matrix_from_network(b: u8, v1: u32, v2: u32) -> Rot3 {
    let r1 = convert_rot_column_from_network(b, v1);
    let r2 = convert_rot_column_from_network(b, v2);
    let r0 = r1.cross(r2);
    Rot3::from_rotation_axes(r0, r1, r2)
}

#[allow(dead_code)]
#[inline]
fn convert_rot_column_from_network(b: u8, v: u32) -> Vec3 {
    let start = v & 7;

    let [mut temp1, mut temp2, mut temp3] = TABLE[start as usize];
    let mut pos = 3;
    while pos < b {
        let step = (v >> pos) & 3;
        match step {
            0 => {
                temp2 = (temp1 + temp2).normalize();
                temp3 = (temp1 + temp3).normalize();
            }
            1 => {
                temp1 = (temp1 + temp2).normalize();
                temp3 = (temp2 + temp3).normalize();
            }
            2 => {
                temp1 = (temp1 + temp3).normalize();
                temp2 = (temp2 + temp3).normalize();
            }
            3 => {
                let c1 = (temp1 + temp2).normalize();
                let c2 = (temp2 + temp3).normalize();
                let c3 = (temp1 + temp3).normalize();
                temp1 = c1;
                temp2 = c2;
                temp3 = c3;
            }
            _ => panic!(),
        }

        pos += 2;
    }
    (temp1 + temp2 + temp3).normalize()
}

#[inline]
fn convert_rot_column_to_network(b: u8, v: Vec3) -> u32 {
    let oct = (v[0] < 0.0) as u32 | ((v[2] < 0.0) as u32) << 1 | ((v[1] < 0.0) as u32) << 2;

    let [mut t1, mut t2, mut t3] = TABLE[oct as usize];
    let mut res = oct;

    for i in (3..b).step_by(2) {
        let s4 = t1 + t2;
        let s5 = t2 + t3;
        let s6 = t1 + t3;

        // The normalization factors are positive, so they do not affect the
        // signs used to select a child triangle.
        if s6.cross(s4).dot(v) < 0.0 {
            if s4.cross(s5).dot(v) < 0.0 {
                if s5.cross(s6).dot(v) < 0.0 {
                    res |= 3 << i;
                    t1 = s4.normalize();
                    t2 = s5.normalize();
                    t3 = s6.normalize();
                } else {
                    res |= 2 << i;
                    t1 = s6.normalize();
                    t2 = s5.normalize();
                }
            } else {
                res |= 1 << i;
                t1 = s4.normalize();
                t3 = s5.normalize();
            }
        } else {
            t2 = s4.normalize();
            t3 = s6.normalize();
        }
    }
    res
}

pub struct HQMMessageWriter<'a> {
    buf: &'a mut BytesMut,
    bit_pos: u8,
}

impl<'a> HQMMessageWriter<'a> {
    pub fn write_byte_aligned(&mut self, v: u8) {
        self.bit_pos = 0;
        self.buf.put_u8(v);
    }

    pub fn write_bytes_aligned(&mut self, v: &[u8]) {
        self.bit_pos = 0;
        self.buf.put_slice(v);
    }

    pub fn write_bytes_aligned_padded(&mut self, n: usize, v: &[u8]) {
        self.bit_pos = 0;
        let m = min(n, v.len());
        self.buf.put_slice(&v[0..m]);
        if n > m {
            self.buf.put_bytes(0, n - m);
        }
    }

    pub fn write_u32_aligned(&mut self, v: u32) {
        self.bit_pos = 0;
        self.buf.put_u32_le(v);
    }

    #[allow(dead_code)]
    pub fn write_f32_aligned(&mut self, v: f32) {
        self.write_u32_aligned(f32::to_bits(v));
    }

    pub fn write_pos(&mut self, n: u8, v: u32, old_v: Option<u32>) {
        let diff = match old_v {
            Some(old_v) => (v as i32) - (old_v as i32),
            None => i32::MAX,
        };

        if (-2i32.pow(2)..=2i32.pow(2) - 1).contains(&diff) {
            self.write_bits(2, 0);
            self.write_bits(3, diff as u32);
        } else if (-2i32.pow(5)..=2i32.pow(5) - 1).contains(&diff) {
            self.write_bits(2, 1);
            self.write_bits(6, diff as u32);
        } else if (-2i32.pow(11)..=2i32.pow(11) - 1).contains(&diff) {
            self.write_bits(2, 2);
            self.write_bits(12, diff as u32);
        } else {
            self.write_bits(2, 3);
            self.write_bits(n, v);
        }
    }

    pub fn write_bits(&mut self, n: u8, v: u32) {
        debug_assert!(n <= 32);
        let v = if n < 32 { v & !(u32::MAX << n) } else { v };
        let staged: u64 = (v as u64) << self.bit_pos;
        let total_bits = self.bit_pos + n;
        let total_bytes = (total_bits + 7) / 8;

        if self.bit_pos > 0 {
            *self.buf.last_mut().unwrap() |= staged as u8;
        }
        let start = if self.bit_pos > 0 { 1 } else { 0 };
        for i in start..total_bytes as usize {
            self.buf.put_u8((staged >> (i * 8)) as u8);
        }

        self.bit_pos = total_bits % 8;
    }

    pub fn recording_fix(&mut self) {
        if self.bit_pos == 0 {
            self.buf.put_u8(0);
        }
    }

    pub fn new(buf: &'a mut BytesMut) -> Self {
        HQMMessageWriter { buf, bit_pos: 0 }
    }
}

pub struct HQMMessageReader<'a> {
    buf: &'a [u8],
    pos: usize,
    bit_pos: u8,
}

impl<'a> HQMMessageReader<'a> {
    #[allow(dead_code)]
    pub fn get_pos(&self) -> usize {
        self.pos
    }

    fn safe_get_byte(&self, pos: usize) -> u8 {
        if pos < self.buf.len() {
            self.buf[pos]
        } else {
            0
        }
    }

    pub fn read_byte_aligned(&mut self) -> u8 {
        self.align();
        let res = self.safe_get_byte(self.pos);
        self.pos += 1;
        res
    }

    pub fn read_bytes_aligned(&mut self, out: &mut [u8]) {
        self.align();
        let n = out.len();

        for i in 0..n {
            out[i] = self.safe_get_byte(self.pos + i)
        }
        self.pos += n;
    }

    pub fn read_u16_aligned(&mut self) -> u16 {
        self.align();
        let bytes = [
            self.safe_get_byte(self.pos),
            self.safe_get_byte(self.pos + 1),
        ];
        self.pos += 2;
        u16::from_le_bytes(bytes)
    }

    pub fn read_u32_aligned(&mut self) -> u32 {
        self.align();
        let bytes = [
            self.safe_get_byte(self.pos),
            self.safe_get_byte(self.pos + 1),
            self.safe_get_byte(self.pos + 2),
            self.safe_get_byte(self.pos + 3),
        ];
        self.pos += 4;
        u32::from_le_bytes(bytes)
    }

    pub fn read_f32_aligned(&mut self) -> f32 {
        let i = self.read_u32_aligned();
        f32::from_bits(i)
    }

    #[allow(dead_code)]
    pub fn read_pos(&mut self, b: u8, old_value: Option<u32>) -> u32 {
        let pos_type = self.read_bits(2);
        match pos_type {
            0 => {
                let diff = self.read_bits_signed(3);
                let old_value = old_value.unwrap() as i32;
                (old_value + diff).max(0) as u32
            }
            1 => {
                let diff = self.read_bits_signed(6);
                let old_value = old_value.unwrap() as i32;
                (old_value + diff).max(0) as u32
            }
            2 => {
                let diff = self.read_bits_signed(12);
                let old_value = old_value.unwrap() as i32;
                (old_value + diff).max(0) as u32
            }
            3 => self.read_bits(b),
            _ => panic!(),
        }
    }

    #[allow(dead_code)]
    pub fn read_bits_signed(&mut self, b: u8) -> i32 {
        let a = self.read_bits(b);

        if a >= 1 << (b - 1) {
            (-1 << b) | (a as i32)
        } else {
            a as i32
        }
    }

    pub fn read_bits(&mut self, b: u8) -> u32 {
        debug_assert!(b <= 32);

        let total_bytes = ((self.bit_pos + b + 7) / 8) as usize;

        let mut staged = 0u64;
        for i in 0..total_bytes {
            let byte = self.buf.get(self.pos + i).copied().unwrap_or(0);
            staged |= (byte as u64) << (i * 8);
        }

        staged >>= self.bit_pos;
        let res = if b < 32 {
            (staged as u32) & !(u32::MAX << b)
        } else {
            staged as u32
        };

        self.bit_pos += b;
        self.pos += (self.bit_pos / 8) as usize;
        self.bit_pos %= 8;

        res
    }

    pub fn align(&mut self) {
        if self.bit_pos > 0 {
            self.bit_pos = 0;
            self.pos += 1;
        }
    }

    #[allow(dead_code)]
    pub fn next(&mut self) {
        self.pos += 1;
        self.bit_pos = 0;
    }

    pub fn new(buf: &'a [u8]) -> Self {
        HQMMessageReader {
            buf,
            pos: 0,
            bit_pos: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ObjectPacket {
    None,
    Puck(PuckPacket),
    Skater(SkaterPacket),
}

#[derive(Debug, Clone)]
pub(crate) struct SkaterPacket {
    pub pos: (u32, u32, u32),
    pub rot: (u32, u32),
    pub stick_pos: (u32, u32, u32),
    pub stick_rot: (u32, u32),
    pub head_rot: u32,
    pub body_rot: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct PuckPacket {
    pub pos: (u32, u32, u32),
    pub rot: (u32, u32),
}

pub(crate) fn write_message(writer: &mut HQMMessageWriter, message: &HQMMessage) {
    match message {
        HQMMessage::Chat {
            player_index,
            message,
        } => {
            writer.write_bits(6, 2);
            writer.write_bits(
                6,
                match *player_index {
                    Some(x) => x.0 as u32,
                    None => u32::MAX,
                },
            );
            let message_bytes = message.as_bytes();
            let size = min(63, message_bytes.len());
            writer.write_bits(6, size as u32);

            for b in message_bytes.iter().take(size).copied() {
                writer.write_bits(7, b as u32);
            }
        }
        HQMMessage::Goal {
            team,
            goal_player_index,
            assist_player_index,
        } => {
            writer.write_bits(6, 1);
            writer.write_bits(2, team.get_num());
            writer.write_bits(
                6,
                match *goal_player_index {
                    Some(x) => x.0 as u32,
                    None => u32::MAX,
                },
            );
            writer.write_bits(
                6,
                match *assist_player_index {
                    Some(x) => x.0 as u32,
                    None => u32::MAX,
                },
            );
        }
        HQMMessage::PlayerUpdate { player_index, data } => {
            writer.write_bits(6, 0);
            writer.write_bits(6, player_index.0 as u32);

            let (in_server, name_bytes) = match data {
                None => (false, &[] as &[u8]),
                Some(p) => (true, p.player_name.as_bytes()),
            };
            let (object_index, team_num) = match data.as_ref().and_then(|x| x.object) {
                Some((slot, team)) => (slot.index() as u32, team.get_num()),
                None => (u32::MAX, u32::MAX),
            };
            writer.write_bits(1, if in_server { 1 } else { 0 });
            writer.write_bits(2, team_num);
            writer.write_bits(6, object_index);

            for i in 0usize..31 {
                let v = if i < name_bytes.len() {
                    name_bytes[i]
                } else {
                    0
                };
                writer.write_bits(7, v as u32);
            }
        }
    };
}

pub(crate) fn write_objects(
    writer: &mut HQMMessageWriter,
    history: &PacketHistory,
    known_packet: Option<PacketNumber>,
) {
    let current_packets = history.current_objects();
    let old_packets = known_packet.and_then(|packet| history.baseline_objects_for(packet));

    writer.write_u32_aligned(history.latest_sequence().to_wire());
    writer.write_u32_aligned(known_packet.map_or(u32::MAX, PacketNumber::to_wire));

    for i in 0..32 {
        let current_packet = &current_packets[i];
        let old_packet = old_packets.map(|x| &x[i]);
        match current_packet {
            ObjectPacket::Puck(puck) => {
                let old_puck = old_packet.and_then(|x| match x {
                    ObjectPacket::Puck(old_puck) => Some(old_puck),
                    _ => None,
                });
                writer.write_bits(1, 1);
                writer.write_bits(2, 1); // Puck type
                writer.write_pos(17, puck.pos.0, old_puck.map(|puck| puck.pos.0));
                writer.write_pos(17, puck.pos.1, old_puck.map(|puck| puck.pos.1));
                writer.write_pos(17, puck.pos.2, old_puck.map(|puck| puck.pos.2));
                writer.write_pos(31, puck.rot.0, old_puck.map(|puck| puck.rot.0));
                writer.write_pos(31, puck.rot.1, old_puck.map(|puck| puck.rot.1));
            }
            ObjectPacket::Skater(skater) => {
                let old_skater = old_packet.and_then(|x| match x {
                    ObjectPacket::Skater(old_skater) => Some(old_skater),
                    _ => None,
                });
                writer.write_bits(1, 1);
                writer.write_bits(2, 0); // Skater type
                writer.write_pos(17, skater.pos.0, old_skater.map(|skater| skater.pos.0));
                writer.write_pos(17, skater.pos.1, old_skater.map(|skater| skater.pos.1));
                writer.write_pos(17, skater.pos.2, old_skater.map(|skater| skater.pos.2));
                writer.write_pos(31, skater.rot.0, old_skater.map(|skater| skater.rot.0));
                writer.write_pos(31, skater.rot.1, old_skater.map(|skater| skater.rot.1));
                writer.write_pos(
                    13,
                    skater.stick_pos.0,
                    old_skater.map(|skater| skater.stick_pos.0),
                );
                writer.write_pos(
                    13,
                    skater.stick_pos.1,
                    old_skater.map(|skater| skater.stick_pos.1),
                );
                writer.write_pos(
                    13,
                    skater.stick_pos.2,
                    old_skater.map(|skater| skater.stick_pos.2),
                );
                writer.write_pos(
                    25,
                    skater.stick_rot.0,
                    old_skater.map(|skater| skater.stick_rot.0),
                );
                writer.write_pos(
                    25,
                    skater.stick_rot.1,
                    old_skater.map(|skater| skater.stick_rot.1),
                );
                writer.write_pos(
                    16,
                    skater.head_rot,
                    old_skater.map(|skater| skater.head_rot),
                );
                writer.write_pos(
                    16,
                    skater.body_rot,
                    old_skater.map(|skater| skater.body_rot),
                );
            }
            ObjectPacket::None => {
                writer.write_bits(1, 0);
            }
        }
    }
}
