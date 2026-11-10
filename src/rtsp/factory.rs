use gstreamer::ClockTime;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, Caps, Element, ElementFactory, FlowError, GhostPad};
use gstreamer_app::{AppSrc, AppSrcCallbacks, AppStreamType};
use gstreamer_rtsp_server::{prelude::*, RTSPMedia};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{
        BcMedia, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe, VideoType,
    },
};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::{sync::mpsc::channel as mpsc, task::JoinHandle};

use crate::{common::NeoInstance, rtsp::gst::NeoMediaFactory, AnyResult};

#[derive(Clone, Debug)]
pub enum AudioType {
    Aac,
    Adpcm(u32),
}

#[derive(Clone, Debug)]
struct StreamConfig {
    #[allow(dead_code)]
    resolution: [u32; 2],
    bitrate: u32,
    fps: u32,
    bitrate_table: Vec<u32>,
    fps_table: Vec<u32>,
    vid_type: Option<VideoType>,
    aud_type: Option<AudioType>,
}
impl StreamConfig {
    async fn new(instance: &NeoInstance, name: StreamKind) -> AnyResult<Self> {
        let (resolution, bitrate, fps, fps_table, bitrate_table) = instance
            .run_passive_task(|cam| {
                Box::pin(async move {
                    let infos = cam
                        .get_stream_info()
                        .await?
                        .stream_infos
                        .iter()
                        .flat_map(|info| info.encode_tables.clone())
                        .collect::<Vec<_>>();
                    if let Some(encode) =
                        infos.iter().find(|encode| encode.name == name.to_string())
                    {
                        let bitrate_table = encode
                            .bitrate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();
                        let framerate_table = encode
                            .framerate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();

                        Ok((
                            [encode.resolution.width, encode.resolution.height],
                            bitrate_table
                                .get(encode.default_bitrate as usize)
                                .copied()
                                .unwrap_or(encode.default_bitrate)
                                * 1024,
                            framerate_table
                                .get(encode.default_framerate as usize)
                                .copied()
                                .unwrap_or(encode.default_framerate),
                            framerate_table.clone(),
                            bitrate_table.clone(),
                        ))
                    } else {
                        Ok(([0, 0], 0, 0, vec![], vec![]))
                    }
                })
            })
            .await?;

        Ok(StreamConfig {
            resolution,
            bitrate,
            fps,
            fps_table,
            bitrate_table,
            vid_type: None,
            aud_type: None,
        })
    }

    fn update_fps(&mut self, fps: u32) {
        let new_fps = self.fps_table.get(fps as usize).copied().unwrap_or(fps);
        self.fps = new_fps;
    }
    #[allow(dead_code)]
    fn update_bitrate(&mut self, bitrate: u32) {
        let new_bitrate = self
            .bitrate_table
            .get(bitrate as usize)
            .copied()
            .unwrap_or(bitrate);
        self.bitrate = new_bitrate;
    }

    fn update_from_media(&mut self, media: &BcMedia) {
        match media {
            BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
            | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) => self.update_fps(*fps as u32),
            BcMedia::Aac(_) => {
                self.aud_type = Some(AudioType::Aac);
            }
            BcMedia::Adpcm(adpcm) => {
                self.aud_type = Some(AudioType::Adpcm(adpcm.block_size()));
            }
            BcMedia::Iframe(BcMediaIframe { video_type, .. })
            | BcMedia::Pframe(BcMediaPframe { video_type, .. }) => {
                self.vid_type = Some(*video_type);
            }
        }
    }
}

pub(super) async fn make_dummy_factory(
    use_splash: bool,
    pattern: String,
) -> AnyResult<NeoMediaFactory> {
    NeoMediaFactory::new_with_callback(move |element| {
        clear_bin(&element)?;
        if !use_splash {
            Ok(None)
        } else {
            build_unknown(&element, &pattern)?;
            Ok(Some(element))
        }
    })
    .await
}

/// How long a connecting RTSP client will wait for its pipeline to be built
/// before we give up. This bounds the time the gst-rtsp-server client thread
/// can be blocked inside the media factory callback. Without a bound, one
/// camera that will not deliver frames wedges that thread forever, and every
/// subsequent client on every camera queues behind it.
const CLIENT_BUILD_TIMEOUT: Duration = Duration::from_secs(15);

/// How long to wait for enough frames to identify the stream format before
/// giving up and building whatever we know about. Must be comfortably less
/// than `CLIENT_BUILD_TIMEOUT`.
const STREAM_LEARN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often an idle frame-pusher thread wakes to check whether its pipeline
/// still exists. Bounds how long a thread survives after its client has gone
/// away without the camera sending another frame.
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Backoff between attempts to resubscribe to the camera after its stream
/// ends while a client is still watching. Doubles up to the max.
const RESUBSCRIBE_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RESUBSCRIBE_BACKOFF_MAX: Duration = Duration::from_secs(10);
/// Longest wait for one resubscribe attempt, so the pusher keeps checking
/// whether its client is still there while the camera is unreachable.
const RESUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(15);

/// True while an appsrc is still attached to a live pipeline.
fn source_alive(src: &AppSrc) -> bool {
    let element: &Element = src.upcast_ref();
    element.parent().is_some() && element.current_state() != gstreamer::State::Null
}

/// True while at least one of this client's sources is still live.
fn sources_alive(vid: &Option<AppSrc>, aud: &Option<AppSrc>) -> bool {
    vid.as_ref().is_some_and(source_alive) || aud.as_ref().is_some_and(source_alive)
}

/// The media that a pipeline ends up in. It is filled in by the factory's
/// media-configure handler, which runs after the pipeline has been built.
type MediaSlot = Arc<OnceLock<gstreamer::glib::WeakRef<RTSPMedia>>>;

/// Runs when a frame pusher exits, however it exits. Nothing will feed this
/// media again, so end its streams and stop handing it to new clients;
/// otherwise the shared media stays cached and every later client joins a
/// pipeline that never gets another frame.
struct PusherExit {
    vid_src: Option<AppSrc>,
    aud_src: Option<AppSrc>,
    media: MediaSlot,
}

impl Drop for PusherExit {
    fn drop(&mut self) {
        for src in self.vid_src.iter().chain(self.aud_src.iter()) {
            let _ = src.end_of_stream();
        }
        if let Some(media) = self.media.get().and_then(|media| media.upgrade()) {
            media.set_shared(false);
        }
    }
}

enum ClientMsg {
    NewClient {
        element: Element,
        /// A std channel rather than a tokio oneshot: the receiving end is
        /// waited on from the gst-rtsp-server client thread, which is not a
        /// tokio thread, and `recv_timeout` needs no runtime handle.
        reply: std::sync::mpsc::SyncSender<Element>,
        media: MediaSlot,
    },
}

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn(async move {
        let name = camera.config().await?.borrow().name.clone();

        while let Some(msg) = client_rx.recv().await {
            match msg {
                ClientMsg::NewClient {
                    element,
                    reply,
                    media,
                } => {
                    log::debug!("New client for {name}::{stream}");
                    let camera = camera.clone();
                    let name = name.clone();
                    tokio::task::spawn(async move {
                        clear_bin(&element)?;
                        log::trace!("{name}::{stream}: Starting camera");

                        // Start the camera
                        let mut media_rx = camera.stream_while_live(stream).await?;

                        log::trace!("{name}::{stream}: Learning camera stream type");
                        // Learn the camera data type
                        let mut buffer = vec![];
                        let mut frame_count = 0usize;

                        let mut stream_config = StreamConfig::new(&camera, stream).await?;
                        // Bounded: a camera that never delivers must not hold
                        // the client thread that is waiting on `reply`.
                        let learn_deadline = tokio::time::Instant::now() + STREAM_LEARN_TIMEOUT;
                        loop {
                            match tokio::time::timeout_at(learn_deadline, media_rx.recv()).await {
                                Ok(Some(media)) => {
                                    stream_config.update_from_media(&media);
                                    buffer.push(media);
                                    if frame_count > 10
                                        || (stream_config.vid_type.is_some()
                                            && stream_config.aud_type.is_some())
                                    {
                                        break;
                                    }
                                    frame_count += 1;
                                }
                                Ok(None) => {
                                    log::debug!(
                                        "{name}::{stream}: Camera stream closed while learning format"
                                    );
                                    break;
                                }
                                Err(_) => {
                                    log::warn!(
                                        "{name}::{stream}: Timed out after {}s learning stream format ({} frames); continuing with what is known",
                                        STREAM_LEARN_TIMEOUT.as_secs(),
                                        frame_count
                                    );
                                    break;
                                }
                            }
                        }

                        log::trace!("{name}::{stream}: Building the pipeline");
                        // Build the right video pipeline
                        let vid_src = match stream_config.vid_type.as_ref() {
                            Some(VideoType::H264) => {
                                let src = build_h264(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(VideoType::H265) => {
                                let src = build_h265(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => {
                                // A splash here would be cached as the shared
                                // media and end after its test buffers, so
                                // every later client would get a dead stream.
                                // Refuse this client instead; it will retry.
                                log::warn!(
                                    "{name}::{stream}: No video while learning stream format; refusing client"
                                );
                                return AnyResult::Ok(());
                            }
                        }?;

                        // Build the right audio pipeline
                        let aud_src = match stream_config.aud_type.as_ref() {
                            Some(AudioType::Aac) => {
                                let src = build_aac(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(AudioType::Adpcm(block_size)) => {
                                let src = build_adpcm(&element, *block_size, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => AnyResult::Ok(None),
                        }?;

                        if let Some(app) = vid_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }
                        if let Some(app) = aud_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }

                        log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
                        // Send the pipeline back to the factory so it can start
                        // If this fails the client already gave up waiting, so
                        // returning here drops `media_rx` and releases the
                        // camera's use-counter permit instead of leaking both
                        // the stream and the pusher thread below.
                        if reply.send(element).is_err() {
                            log::debug!(
                                "{name}::{stream}: Client gave up before the pipeline was ready; releasing stream"
                            );
                            return AnyResult::Ok(());
                        }

                        // Run blocking code on a seperate thread
                        // This is not an async thread
                        // The handle lets the thread wait on the async channel
                        // with a timeout; a plain std thread is never inside an
                        // async context, so block_on is safe here.
                        let runtime = tokio::runtime::Handle::current();
                        std::thread::spawn(move || {
                            let _exit = PusherExit {
                                vid_src: vid_src.clone(),
                                aud_src: aud_src.clone(),
                                media,
                            };
                            let mut clock = MediaClock::new(stream_config.fps);
                            let mut stats = TsStats::new(format!("{name}::{stream}"));
                            // P-frames are dropped until a keyframe has been
                            // pushed: at start, after any dropped frame and
                            // after resubscribing to the camera.
                            let mut need_keyframe = true;

                            log::trace!("{name}::{stream}: Sending buffered frames");
                            for buffered in buffer.drain(..) {
                                send_to_sources(
                                    buffered,
                                    &vid_src,
                                    &aud_src,
                                    &mut clock,
                                    &stream_config,
                                    &mut stats,
                                    &mut need_keyframe,
                                )?;
                            }

                            log::trace!("{name}::{stream}: Sending new frames");
                            let mut backoff = RESUBSCRIBE_BACKOFF_MIN;
                            // The pipeline is still NULL while gst-rtsp-server
                            // prepares it, so only a pipeline that has been
                            // live and then went back to NULL counts as gone.
                            let mut was_live = false;
                            'frames: loop {
                                // Bounded wait so that a client that has gone
                                // away is noticed even if the camera has also
                                // stopped sending. An unbounded blocking_recv
                                // here strands the thread, and with it the
                                // camera stream it holds open, until restart.
                                let next = runtime.block_on(async {
                                    tokio::time::timeout(IDLE_CHECK_INTERVAL, media_rx.recv()).await
                                });
                                let data = match next {
                                    Ok(Some(data)) => {
                                        backoff = RESUBSCRIBE_BACKOFF_MIN;
                                        data
                                    }
                                    Ok(None) => {
                                        // The camera side ended (relogin,
                                        // dropped connection) but the client
                                        // is still watching. Subscribe again
                                        // rather than leave it on a media
                                        // that will never get another frame.
                                        loop {
                                            std::thread::sleep(backoff);
                                            backoff = (backoff * 2).min(RESUBSCRIBE_BACKOFF_MAX);
                                            if !sources_alive(&vid_src, &aud_src) {
                                                break 'frames;
                                            }
                                            // Bounded, so a client that leaves
                                            // while the camera is offline is
                                            // still noticed on the next pass.
                                            match runtime.block_on(tokio::time::timeout(
                                                RESUBSCRIBE_TIMEOUT,
                                                camera.stream_while_live(stream),
                                            )) {
                                                Ok(Ok(new_rx)) => {
                                                    log::debug!(
                                                        "{name}::{stream}: Camera stream resubscribed"
                                                    );
                                                    media_rx = new_rx;
                                                    need_keyframe = true;
                                                    clock.camera_restarted();
                                                    continue 'frames;
                                                }
                                                Ok(Err(e)) => {
                                                    log::debug!(
                                                        "{name}::{stream}: Camera stream resubscribe failed: {e:?}"
                                                    );
                                                }
                                                Err(_) => {
                                                    log::debug!(
                                                        "{name}::{stream}: Camera stream resubscribe timed out"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        if sources_alive(&vid_src, &aud_src) {
                                            continue;
                                        }
                                        log::debug!(
                                            "{name}::{stream}: Pipeline gone while idle; stopping"
                                        );
                                        break;
                                    }
                                };
                                // Follow a frame rate change reported
                                // mid-stream.
                                if let BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
                                | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) = &data
                                {
                                    stream_config.update_fps(*fps as u32);
                                    clock.set_fps(stream_config.fps);
                                }
                                let r = send_to_sources(
                                    data,
                                    &vid_src,
                                    &aud_src,
                                    &mut clock,
                                    &stream_config,
                                    &mut stats,
                                    &mut need_keyframe,
                                );
                                if let Err(r) = &r {
                                    // Normal at teardown: the media was
                                    // unprepared while this thread still had
                                    // frames in flight.
                                    log::debug!("Failed to send to source: {r:?}");
                                }
                                r?;
                                // An unprepared media leaves this bin in a
                                // NULL pipeline where pushes still succeed
                                // (as Flushing), so check after every frame
                                // rather than only when the camera goes idle.
                                // Otherwise this thread keeps the camera
                                // stream open for a pipeline nobody plays.
                                if sources_alive(&vid_src, &aud_src) {
                                    was_live = true;
                                } else if was_live {
                                    log::debug!("{name}::{stream}: Pipeline gone; stopping");
                                    break;
                                }
                            }
                            log::trace!("All media recieved");
                            AnyResult::Ok(())
                        });
                        AnyResult::Ok(())
                    });
                }
            }
        }
        AnyResult::Ok(())
    });

    // gst-rtsp-server builds the element and then configures its media in one
    // call, under the factory's media lock, so a single pending slot is enough
    // to pair each pipeline with its media.
    let pending_media: Arc<Mutex<Option<MediaSlot>>> = Default::default();
    let callback_pending = pending_media.clone();

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(move |element| {
        let (reply, new_element) = std::sync::mpsc::sync_channel::<Element>(1);
        let media = MediaSlot::default();
        if let Ok(mut pending) = callback_pending.lock() {
            pending.replace(media.clone());
        }
        // try_send, not blocking_send: if the builder task is wedged we must
        // fail this client rather than block the shared client thread.
        client_tx
            .try_send(ClientMsg::NewClient {
                element,
                reply,
                media,
            })
            .map_err(|e| anyhow!("Pipeline builder is not accepting clients: {e}"))?;

        match new_element.recv_timeout(CLIENT_BUILD_TIMEOUT) {
            Ok(element) => Ok(Some(element)),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(anyhow!("Pipeline builder gave up without a pipeline"))
            }
            Err(e) => Err(anyhow!(
                "Timed out after {}s waiting for the pipeline: {e}",
                CLIENT_BUILD_TIMEOUT.as_secs()
            )),
        }
    })
    .await?;
    factory.connect_media_configure(move |_, media| {
        if let Some(slot) = pending_media.lock().ok().and_then(|mut p| p.take()) {
            let _ = slot.set(media.downgrade());
        }
    });
    Ok((factory, thread))
}

/// Generates presentation timestamps for both media channels off one origin.
///
/// Each channel advances at its own nominal rate - the frame interval for
/// video, the decoded frame duration for audio - so spacing stays even. Both
/// are then nudged each frame to stay locked to a shared monotonic clock.
///
/// Without the lock, either channel accumulates error for the whole session:
/// a real 14.99fps against a nominal 15 is 667ppm, and audio has the same
/// problem via its sample rate plus any lost frame, which is permanent once
/// it happens. Sharing one origin is what keeps the two channels in sync with
/// each other rather than merely each being close to correct on its own.
struct MediaClock {
    started: std::time::Instant,
    vid_pts_us: u64,
    vid_step_us: u64,
    /// PTS of the last video frame handed out. `vid_pts_us` is already the
    /// next frame's PTS, one step later. None until the first video frame.
    last_vid_pts_us: Option<u64>,
    /// Camera timestamp of the last video frame, for frame spacing.
    last_cam_us: Option<u32>,
    aud_pts_us: u64,
    aud_started: bool,
    /// Extra lead applied to audio, in microseconds. Positive moves audio
    /// earlier. Set at runtime with NEOLINK_AUDIO_LEAD_MS for cameras that
    /// buffer more than a single frame before transmitting.
    aud_lead_us: i64,
    vid_snaps: u64,
    aud_snaps: u64,
}

impl MediaClock {
    /// Video further behind the wall clock than this jumps forward instead
    /// of slewing.
    const VID_SNAP_US: i64 = 1_000_000;
    /// Audio further behind the video timeline than this jumps forward
    /// instead of slewing. Wider than two 64ms AAC frames, so audio that
    /// arrives a few frames at a time does not trip it.
    const AUD_RESYNC_US: i64 = 150_000;

    fn new(fps: u32) -> Self {
        Self {
            started: std::time::Instant::now(),
            vid_pts_us: 0,
            vid_step_us: 1_000_000 / (fps.max(1) as u64),
            last_vid_pts_us: None,
            last_cam_us: None,
            aud_pts_us: 0,
            aud_started: false,
            aud_lead_us: std::env::var("NEOLINK_AUDIO_LEAD_MS")
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .unwrap_or(0)
                .saturating_mul(1000),
            vid_snaps: 0,
            aud_snaps: 0,
        }
    }

    fn wall_us(&self) -> i64 {
        self.started.elapsed().as_micros() as i64
    }

    /// A new camera stream may restart its frame timestamps, so the next
    /// frame is spaced by the nominal step rather than by a bogus delta.
    fn camera_restarted(&mut self) {
        self.last_cam_us = None;
    }

    /// Nominal frame rate, used where the camera gives no frame spacing.
    fn set_fps(&mut self, fps: u32) {
        self.vid_step_us = 1_000_000 / (fps.max(1) as u64);
    }

    /// PTS for the next video frame, in microseconds. `cam_us` is the
    /// camera's timestamp for this frame.
    fn next_video(&mut self, cam_us: u32, name: &str) -> u64 {
        let wall_us = self.wall_us();
        self.next_video_at(wall_us, cam_us, name)
    }

    /// `next_video` with the wall clock passed in, so tests can drive it.
    ///
    /// PTS only ever moves forward. A backward step makes downstream
    /// elements and players drop or reorder frames.
    fn next_video_at(&mut self, wall_us: i64, cam_us: u32, name: &str) -> u64 {
        let pts = self.vid_pts_us;
        let err = pts as i64 - wall_us;

        // Space frames by the camera's own timestamps, so PTS follows the
        // rate the camera actually delivers rather than the configured one.
        // A camera set to 15 fps that sends 12 would otherwise fall behind
        // the wall clock faster than the slew can follow, and snap every
        // few seconds. Clamped so one bad timestamp cannot run away.
        let nominal = self.vid_step_us as i64;
        let step = match self.last_cam_us.replace(cam_us) {
            Some(last) => (cam_us.wrapping_sub(last) as i64).clamp(nominal / 4, nominal * 4),
            None => nominal,
        };

        if err < -Self::VID_SNAP_US {
            // Too far behind to slew: the stream stalled, the camera slept,
            // or we are at startup. Jump rather than compressing real elapsed
            // time into a slow correction.
            self.vid_snaps += 1;
            log::debug!("{name}: video clock snap, was {}ms out", err / 1000);
            let wall = wall_us.max(0) as u64;
            self.vid_pts_us = wall.saturating_add(step as u64);
            self.last_vid_pts_us = Some(wall);
            return wall;
        }

        // Behind: capped at 1% of a frame interval so frame spacing stays
        // even. Ahead, typically after the burst that follows a stall: up to
        // half an interval, so the lead drains in seconds while every PTS
        // still lands after the one before it.
        let corr = (-err / 64).clamp(-(step / 2), (step / 100).max(1));
        self.vid_pts_us = self.vid_pts_us.saturating_add((step + corr).max(1) as u64);
        self.last_vid_pts_us = Some(pts);
        pts
    }

    /// PTS for the next audio frame, in microseconds. `duration_us` is the
    /// decoded length of this frame.
    ///
    /// Audio carries no camera timestamp - `BcMediaAac` and `BcMediaAdpcm`
    /// hold only `data`, unlike the video frames which have `microseconds`.
    /// The camera conveys audio position by interleaving it with video, so
    /// the video timeline is the anchor, NOT the wall clock. Anchoring to
    /// arrival time instead puts audio permanently one frame duration late
    /// and turns every lost packet into accumulated error.
    ///
    /// Returns None until the first video frame, since there is nothing to
    /// anchor to yet. Like video, audio PTS only ever moves forward.
    fn next_audio(&mut self, duration_us: u64, name: &str) -> Option<u64> {
        let last_vid_pts_us = self.last_vid_pts_us?;
        // The frame that has just arrived covers roughly
        // [anchor - duration, anchor] on the video timeline, because the
        // encoder could not emit it until the last of its samples existed.
        let anchor = (last_vid_pts_us as i64)
            .saturating_sub(duration_us as i64)
            .saturating_sub(self.aud_lead_us)
            .max(0);

        if !self.aud_started {
            self.aud_started = true;
            self.aud_pts_us = anchor as u64;
        }

        let err = self.aud_pts_us as i64 - anchor;

        if err < -Self::AUD_RESYNC_US {
            // Audio frames went missing, so there is a real gap here. Jumping
            // to the anchor renders it as the short silence it actually is.
            // Slewing instead would compress the surviving audio and hold the
            // desync for minutes.
            self.aud_snaps += 1;
            log::debug!(
                "{name}: audio resync, was {}ms off the video clock",
                err / 1000
            );
            self.aud_pts_us = anchor as u64;
            let pts = self.aud_pts_us;
            self.aud_pts_us = self.aud_pts_us.saturating_add(duration_us);
            return Some(pts);
        }

        // Otherwise stay contiguous and slew out the residual. Audio ahead of
        // the anchor is normal when frames arrive a few at a time, so it is
        // never jumped back. The cap of 1/8 of a frame keeps every PTS after
        // the one before it.
        let max_corr = (duration_us / 8).max(1) as i64;
        let corr = (-err / 64).clamp(-max_corr, max_corr);
        let pts = self.aud_pts_us;
        self.aud_pts_us = self
            .aud_pts_us
            .saturating_add((duration_us as i64 + corr).max(1) as u64);
        Some(pts)
    }
}

/// Frames discarded by the appsrc queue cap, across all streams.
/// Diagnostic only.
static DROPPED_FRAMES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Tracks how far the synthesised presentation timestamps have drifted from
/// real elapsed time. Purely diagnostic - it reads state, never changes it.
struct TsStats {
    name: String,
    started: std::time::Instant,
    last_report: std::time::Instant,
    vid_frames: u64,
    aud_frames: u64,
    last_aud_pts_us: u64,
    aud_covered_us: u64,
    drops_at_last_report: u64,
}

impl TsStats {
    fn new(name: String) -> Self {
        let now = std::time::Instant::now();
        Self {
            name,
            started: now,
            last_report: now,
            vid_frames: 0,
            aud_frames: 0,
            last_aud_pts_us: 0,
            aud_covered_us: 0,
            drops_at_last_report: DROPPED_FRAMES.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    fn note_audio(&mut self, pts_us: u64, duration_us: u64) {
        self.aud_frames += 1;
        self.last_aud_pts_us = pts_us;
        self.aud_covered_us = self.aud_covered_us.saturating_add(duration_us);
    }

    fn note_video(&mut self, cam_us: u32, pts_us: u64, nominal_fps: u32, clock: &MediaClock) {
        self.vid_frames += 1;
        if self.last_report.elapsed() < std::time::Duration::from_secs(60) {
            return;
        }
        self.last_report = std::time::Instant::now();

        let wall_us = self.started.elapsed().as_micros() as i128;
        let drift_ms = (pts_us as i128 - wall_us) / 1000;
        let secs = self.started.elapsed().as_secs_f64();
        let real_fps = if secs > 0.0 {
            self.vid_frames as f64 / secs
        } else {
            0.0
        };
        let drops = DROPPED_FRAMES.load(std::sync::atomic::Ordering::Relaxed);
        let new_drops = drops.saturating_sub(self.drops_at_last_report);
        self.drops_at_last_report = drops;

        // Skew between the two channels at this instant. Negative means the
        // audio timeline sits behind the video timeline, i.e. audio plays
        // early. This is the number to watch for A/V sync.
        let av_skew_ms = (self.last_aud_pts_us as i128 - pts_us as i128) / 1000;
        // Fraction of elapsed time actually covered by received audio.
        // Below 100% means audio frames are being lost in transit, which is a
        // network problem and not a timestamping one.
        let aud_coverage = if wall_us > 0 {
            (self.aud_covered_us as f64 / wall_us as f64) * 100.0
        } else {
            0.0
        };

        log::info!(
            "{}: uptime {:.0}s | pts {:.1}s vs wall {:.1}s | drift {:+}ms | a/v skew {:+}ms | fps real {:.2} nominal {} | aud frames {} cover {:.1}% | camera ts {:.1}s | dropped {} (+{}) | snaps v{} a{}",
            self.name,
            secs,
            pts_us as f64 / 1e6,
            wall_us as f64 / 1e6,
            drift_ms,
            av_skew_ms,
            real_fps,
            nominal_fps,
            self.aud_frames,
            aud_coverage,
            cam_us as f64 / 1e6,
            drops,
            new_drops,
            clock.vid_snaps,
            clock.aud_snaps,
        );
    }
}

fn send_to_sources(
    data: BcMedia,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    clock: &mut MediaClock,
    stream_config: &StreamConfig,
    stats: &mut TsStats,
    need_keyframe: &mut bool,
) -> AnyResult<()> {
    let keyframe = matches!(data, BcMedia::Iframe(_));
    // Update TS
    match data {
        BcMedia::Aac(aac) => {
            let duration = aac.duration().expect("Could not calculate AAC duration");
            if let Some(pts) = clock.next_audio(duration as u64, &stats.name) {
                if let Some(aud_src) = aud_src.as_ref() {
                    log::debug!("Sending AAC: {:?}", Duration::from_micros(pts));
                    send_to_appsrc(aud_src, aac.data, Duration::from_micros(pts))?;
                }
                stats.note_audio(pts, duration as u64);
            }
        }
        BcMedia::Adpcm(adpcm) => {
            let duration = adpcm
                .duration()
                .expect("Could not calculate ADPCM duration");
            if let Some(pts) = clock.next_audio(duration as u64, &stats.name) {
                if let Some(aud_src) = aud_src.as_ref() {
                    log::trace!("Sending ADPCM: {:?}", Duration::from_micros(pts));
                    send_to_appsrc(aud_src, adpcm.data, Duration::from_micros(pts))?;
                }
                stats.note_audio(pts, duration as u64);
            }
        }
        BcMedia::Iframe(BcMediaIframe {
            data, microseconds, ..
        })
        | BcMedia::Pframe(BcMediaPframe {
            data, microseconds, ..
        }) => {
            let pts = clock.next_video(microseconds, &stats.name);
            if let Some(vid_src) = vid_src.as_ref() {
                // A P-frame after a missing frame decodes against the wrong
                // reference and smears until the next keyframe, so wait for
                // that keyframe instead.
                if keyframe || !*need_keyframe {
                    log::trace!("Sending VID: {:?}", Duration::from_micros(pts));
                    *need_keyframe = !send_to_appsrc(vid_src, data, Duration::from_micros(pts))?;
                }
            }
            stats.note_video(microseconds, pts, stream_config.fps, clock);
        }
        _ => {}
    }
    Ok(())
}

/// How much slack we allow on top of the appsrc's configured `max-bytes`
/// before we start dropping frames.
///
/// `appsrc` is configured with `block = false`, which means `push_buffer`
/// will keep appending to its internal queue even once `max-bytes` is
/// exceeded (it only emits `enough-data`, which nothing here listens for).
/// Without an explicit cap a client that stops consuming makes that queue
/// grow without limit.
const APPSRC_QUEUE_SLACK: u64 = 4;

/// Returns whether the frame was pushed; false when it was dropped.
fn send_to_appsrc(appsrc: &AppSrc, data: Vec<u8>, mut ts: Duration) -> AnyResult<bool> {
    check_live(appsrc)?; // Stop if appsrc is dropped

    // In live mode we follow the advice in
    // https://gstreamer.freedesktop.org/documentation/additional/design/element-source.html?gi-language=c#live-sources
    // Only push buffers when in play state and have a clock
    // we also timestamp at the current time
    if appsrc.is_live() {
        if let Some(time) = appsrc
            .current_clock_time()
            .and_then(|t| appsrc.base_time().map(|bt| t - bt))
        {
            if matches!(appsrc.current_state(), gstreamer::State::Playing) {
                ts = Duration::from_micros(time.useconds());
            } else {
                // Not playing
                return Ok(false);
            }
        } else {
            // Clock not up yet
            return Ok(false);
        }
    }
    // Don't let the appsrc queue grow without bound if the client stalls.
    // This is a live camera feed: dropping is correct, growing is not.
    let max_bytes = appsrc.max_bytes();
    if max_bytes > 0 && appsrc.current_level_bytes() > max_bytes.saturating_mul(APPSRC_QUEUE_SLACK)
    {
        DROPPED_FRAMES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::debug!(
            "{}: appsrc queue at {} bytes (max-bytes {}), dropping frame",
            appsrc.name(),
            appsrc.current_level_bytes(),
            max_bytes,
        );
        return Ok(false);
    }

    // Hand the frame we already own straight to gstreamer.
    //
    // The previous implementation kept a `HashMap<usize, BufferPool>` keyed on
    // the exact byte length of each frame. Compressed frame sizes are
    // effectively unique, so that map gained a new pool for nearly every
    // frame, and each new pool immediately preallocated `min_buffers` buffers
    // of that size. Nothing ever evicted a pool, so the process grew for as
    // long as it streamed. Wrapping the `Vec` we were handed has no such
    // retention, and skips a memcpy as a bonus.
    let buf = {
        let mut new_buf = gstreamer::Buffer::from_mut_slice(data);
        {
            let gst_buf_mut = new_buf
                .get_mut()
                .ok_or_else(|| anyhow!("Freshly created buffer was not uniquely owned"))?;
            let time = ClockTime::from_useconds(ts.as_micros() as u64);
            gst_buf_mut.set_dts(time);
            gst_buf_mut.set_pts(time);
        }
        new_buf
    };

    // Push buffer into the appsrc
    let pushed = match appsrc.push_buffer(buf) {
        Ok(_) => {
            // log::info!(
            //     "Send {}{} on {}",
            //     data.data.len(),
            //     if data.keyframe { " (keyframe)" } else { "" },
            //     appsrc.name()
            // );
            Ok(true)
        }
        Err(FlowError::Flushing) => {
            // NOT "buffer full" - appsrc returns Flushing when the element is
            // in NULL/READY or a flush is in progress, i.e. the pipeline is
            // not running. Log the states so this is diagnosable.
            log::debug!(
                "Flushing on {}: element state {:?}, parent state {:?}. Frame discarded.",
                appsrc.name(),
                appsrc.current_state(),
                appsrc
                    .parent()
                    .and_then(|p| p.downcast::<Element>().ok())
                    .map(|p| p.current_state()),
            );
            Ok(false)
        }
        Err(e) => Err(anyhow!("Error in streaming: {e:?}")),
    }?;
    // NOTE: previously this toggled the appsrc element between PAUSED and
    // PLAYING depending on its fill level. With a client consuming in real
    // time the level sits near empty, so the appsrc spent most of its time
    // PAUSED and released frames in bursts, which shows up as stutter-then-
    // catch-up in viewers. Smoothing is the downstream `queue` element's job;
    // it is bounded, so it cannot grow without limit the way this appsrc can.
    Ok(pushed)
}
fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
}

fn clear_bin(bin: &Element) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    // Clear the autogenerated ones
    for element in bin.iterate_elements().into_iter().flatten() {
        bin.remove(&element)?;
    }

    Ok(())
}

fn build_unknown(bin: &Element, pattern: &str) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Unknown Pipeline");
    let source = make_element("videotestsrc", "testvidsrc")?;
    source.set_property_from_str("pattern", pattern);
    source.set_property("num-buffers", 500i32); // Send buffers then EOS
    let queue = make_queue("queue0", 1024 * 1024 * 4)?;

    let overlay = make_element("textoverlay", "overlay")?;
    overlay.set_property("text", "Stream not Ready");
    overlay.set_property_from_str("valignment", "top");
    overlay.set_property_from_str("halignment", "left");
    overlay.set_property("font-desc", "Sans, 16");
    let encoder = make_element("jpegenc", "encoder")?;
    let payload = make_element("rtpjpegpay", "pay0")?;

    bin.add_many([&source, &queue, &overlay, &encoder, &payload])?;
    source.link_filtered(
        &queue,
        &Caps::builder("video/x-raw")
            .field("format", "YUY2")
            .field("width", 896i32)
            .field("height", 512i32)
            .field("framerate", gstreamer::Fraction::new(25, 1))
            .build(),
    )?;
    Element::link_many([&queue, &overlay, &encoder, &payload])?;

    Ok(())
}

struct Linked {
    appsrc: AppSrc,
    output: Element,
}

fn pipe_h264(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    log::debug!(
        "buffer_size: {buffer_size}, bitrate: {}",
        stream_config.bitrate
    );
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H264 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h264parse", "parser")?;
    // let stamper = make_element("h264timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h264(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h264(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph264pay", "pay0")?;
    // Repeat SPS and PPS with every IDR frame, so a client that joins or
    // loses packets mid-stream can start decoding at the next keyframe.
    payload.set_property("config-interval", -1i32);
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_h265(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H265 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h265parse", "parser")?;
    // let stamper = make_element("h265timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h265(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h265(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph265pay", "pay0")?;
    // Repeat VPS, SPS and PPS with every IDR frame, as for H264.
    payload.set_property("config-interval", -1i32);
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_aac(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let parser = make_element("aacparse", "audparser")?;
    let decoder = match make_element("faad", "auddecoder_faad") {
        Ok(ele) => Ok(ele),
        Err(_) => make_element("avdec_aac", "auddecoder_avdec_aac"),
    }?;

    // The fallback
    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let fallback_switch = make_element("fallbackswitch", "audfallbackswitch");
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        fallback_switch.set_property("timeout", 3u64 * 1_000_000_000u64);
        fallback_switch.set_property("immediate-fallback", true);
    }

    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &queue, &parser, &decoder, &encoder])?;
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        bin.add_many([&silence, fallback_switch])?;
        Element::link_many([
            &source,
            &queue,
            &parser,
            &decoder,
            fallback_switch,
            &encoder,
        ])?;
        Element::link_many([&silence, fallback_switch])?;
    } else {
        Element::link_many([&source, &queue, &parser, &decoder, &encoder])?;
    }

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

fn build_aac(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_aac(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Adpcm pipeline");
    // Original command line
    // caps=audio/x-adpcm,layout=dvi,block_align={},channels=1,rate=8000
    // ! queue silent=true max-size-bytes=10485760 min-threshold-bytes=1024
    // ! adpcmdec
    // ! audioconvert
    // ! rtpL16pay name=pay1

    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    source.set_caps(Some(
        &Caps::builder("audio/x-adpcm")
            .field("layout", "div")
            .field("block_align", block_size as i32)
            .field("channels", 1i32)
            .field("rate", 8000i32)
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let decoder = make_element("decodebin", "auddecoder")?;
    let encoder = make_element("audioconvert", "audencoder")?;
    let encoder_out = encoder.clone();

    bin.add_many([&source, &queue, &decoder, &encoder])?;
    Element::link_many([&source, &queue, &decoder])?;
    decoder.connect_pad_added(move |_element, pad| {
        let sink_pad = encoder
            .static_pad("sink")
            .expect("Encoder is missing its pad");
        pad.link(&sink_pad)
            .expect("Failed to link ADPCM decoder to encoder");
    });

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder_out,
    })
}

fn build_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_adpcm(bin, block_size, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

#[allow(dead_code)]
fn pipe_silence(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Silence pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let sink_queue = make_queue("audsinkqueue", buffer_size)?;
    let sink = make_element("fakesink", "silence_sink")?;

    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let src_queue = make_queue("audsinkqueue", buffer_size)?;
    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &sink_queue, &sink, &silence, &src_queue, &encoder])?;

    Element::link_many([&source, &sink_queue, &sink])?;

    Element::link_many([&silence, &src_queue, &encoder])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

#[allow(dead_code)]
struct AppSrcPair {
    vid: AppSrc,
    aud: Option<AppSrc>,
}

// #[allow(dead_code)]
// /// Experimental build a stream of MPEGTS
// fn build_mpegts(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrcPair> {
//     let buffer_size = buffer_size(stream_config.bitrate);
//     log::debug!(
//         "buffer_size: {buffer_size}, bitrate: {}",
//         stream_config.bitrate
//     );

//     // VID
//     let vid_link = match stream_config.vid_format {
//         VidFormat::H264 => pipe_h264(bin, stream_config)?,
//         VidFormat::H265 => pipe_h265(bin, stream_config)?,
//         VidFormat::None => unreachable!(),
//     };

//     // AUD
//     let aud_link = match stream_config.aud_format {
//         AudFormat::Aac => pipe_aac(bin, stream_config)?,
//         AudFormat::Adpcm(block) => pipe_adpcm(bin, block, stream_config)?,
//         AudFormat::None => pipe_silence(bin, stream_config)?,
//     };

//     let bin = bin
//         .clone()
//         .dynamic_cast::<Bin>()
//         .map_err(|_| anyhow!("Media source's element should be a bin"))?;

//     // MUX
//     let muxer = make_element("mpegtsmux", "mpeg_muxer")?;
//     let rtp = make_element("rtpmp2tpay", "pay0")?;

//     bin.add_many([&muxer, &rtp])?;
//     Element::link_many([&vid_link.output, &muxer, &rtp])?;
//     Element::link_many([&aud_link.output, &muxer])?;

//     Ok(AppSrcPair {
//         vid: vid_link.appsrc,
//         aud: Some(aud_link.appsrc),
//     })
// }

// Convenice funcion to make an element or provide a message
// about what plugin is missing
fn make_element(kind: &str, name: &str) -> AnyResult<Element> {
    ElementFactory::make_with_name(kind, Some(name)).with_context(|| {
        let plugin = match kind {
            "appsrc" => "app (gst-plugins-base)",
            "audioconvert" => "audioconvert (gst-plugins-base)",
            "adpcmdec" => "Required for audio",
            "h264parse" => "videoparsersbad (gst-plugins-bad)",
            "h265parse" => "videoparsersbad (gst-plugins-bad)",
            "rtph264pay" => "rtp (gst-plugins-good)",
            "rtph265pay" => "rtp (gst-plugins-good)",
            "rtpjitterbuffer" => "rtp (gst-plugins-good)",
            "aacparse" => "audioparsers (gst-plugins-good)",
            "rtpL16pay" => "rtp (gst-plugins-good)",
            "x264enc" => "x264 (gst-plugins-ugly)",
            "x265enc" => "x265 (gst-plugins-bad)",
            "avdec_h264" => "libav (gst-libav)",
            "avdec_h265" => "libav (gst-libav)",
            "videotestsrc" => "videotestsrc (gst-plugins-base)",
            "imagefreeze" => "imagefreeze (gst-plugins-good)",
            "audiotestsrc" => "audiotestsrc (gst-plugins-base)",
            "decodebin" => "playback (gst-plugins-good)",
            _ => "Unknown",
        };
        format!(
            "Missing required gstreamer plugin `{}` for `{}` element",
            plugin, kind
        )
    })
}

#[allow(dead_code)]
fn make_dbl_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    // queue.set_property(
    //     "max-size-time",
    //     std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
    //         .unwrap_or(0),
    // );

    let queue2 = make_element("queue2", &format!("queue2_{}", name))?;
    queue2.set_property("max-size-bytes", buffer_size * 2u32 / 3u32);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue2.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    queue2.set_property("use-buffering", false);

    let bin = gstreamer::Bin::builder().name(name).build();
    bin.add_many([&queue, &queue2])?;
    Element::link_many([&queue, &queue2])?;

    let pad = queue
        .static_pad("sink")
        .expect("Failed to get a static pad from queue.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let pad = queue2
        .static_pad("src")
        .expect("Failed to get a static pad from queue2.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let bin = bin
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot convert bin"))?;
    Ok(bin)
}

fn make_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    Ok(queue)
}

fn buffer_size(bitrate: u32) -> u32 {
    // 0.1 seconds (according to bitrate) or 4kb what ever is larger
    std::cmp::max(bitrate * 2 / 8u32, 4u32 * 1024u32)
}

#[cfg(test)]
mod tests {
    use super::MediaClock;

    const AAC_US: u64 = 64_000;

    enum Ev {
        /// A video frame with its camera timestamp.
        Video(u32),
        Audio,
    }

    #[derive(Default)]
    struct Run {
        vid_back: u32,
        aud_back: u32,
        vid_snaps: u64,
        aud_snaps: u64,
    }

    /// Feeds `(wall_us, event)` pairs, in arrival order, through a clock
    /// that nominally runs at 15 fps and counts PTS that fail to increase.
    fn run(mut events: Vec<(i64, Ev)>) -> Run {
        events.sort_by_key(|(wall, _)| *wall);
        let mut clock = MediaClock::new(15);
        clock.aud_lead_us = 0;
        let mut out = Run::default();
        let (mut last_vid, mut last_aud) = (None, None);
        for (wall, ev) in events {
            match ev {
                Ev::Video(cam_us) => {
                    let pts = clock.next_video_at(wall, cam_us, "test");
                    if last_vid.is_some_and(|last| pts <= last) {
                        out.vid_back += 1;
                    }
                    last_vid = Some(pts);
                }
                Ev::Audio => {
                    if let Some(pts) = clock.next_audio(AAC_US, "test") {
                        if last_aud.is_some_and(|last| pts <= last) {
                            out.aud_back += 1;
                        }
                        last_aud = Some(pts);
                    }
                }
            }
        }
        out.vid_snaps = clock.vid_snaps;
        out.aud_snaps = clock.aud_snaps;
        out
    }

    /// Video frames every `interval_us` over `[from_us, to_us)`, each
    /// arriving as soon as the camera stamps it.
    fn video(from_us: i64, to_us: i64, interval_us: i64) -> Vec<(i64, Ev)> {
        (0..)
            .map(|i| from_us + i * interval_us)
            .take_while(|t| *t < to_us)
            .map(|t| (t, Ev::Video(t as u32)))
            .collect()
    }

    /// Audio frames back to back over `(0, to_us)`, each arriving as its
    /// last sample is captured.
    fn audio(to_us: i64) -> Vec<(i64, Ev)> {
        (1..to_us / AAC_US as i64)
            .map(|i| (i * AAC_US as i64, Ev::Audio))
            .collect()
    }

    #[test]
    fn stall_then_burst_never_steps_back() {
        let step = 1_000_000 / 15;
        // 10 s steady, a 2 s stall, the 30 held frames in 100 ms, then 60 s
        // steady. Audio stalls with video and arrives in the same burst.
        let mut events = video(0, 10_000_000, step);
        // The camera kept stamping frames through the stall.
        events.extend((0..30).map(|i| {
            let cam_us = 10_000_000 + i * step;
            (12_000_000 + i * 100_000 / 30, Ev::Video(cam_us as u32))
        }));
        events.extend(video(12_100_000, 72_100_000, step));
        for i in 0..(72_000_000 / AAC_US as i64) {
            let t = (i + 1) * AAC_US as i64;
            let t = if (10_000_000..12_000_000).contains(&t) {
                12_000_000 + (t - 10_000_000) / 20
            } else {
                t
            };
            events.push((t, Ev::Audio));
        }
        let r = run(events);
        assert_eq!(r.vid_back, 0);
        assert_eq!(r.aud_back, 0);
        assert!(r.vid_snaps <= 2, "{} video snaps", r.vid_snaps);
    }

    #[test]
    fn bursty_audio_stays_locked() {
        // Audio arrives three 64 ms frames at a time every 192 ms.
        let mut events = video(0, 60_000_000, 1_000_000 / 15);
        for burst in 1..(60_000_000 / 192_000) {
            events.extend((0..3).map(|_| (burst * 192_000, Ev::Audio)));
        }
        let r = run(events);
        assert_eq!(r.vid_back, 0);
        assert_eq!(r.aud_back, 0);
        assert!(r.aud_snaps <= 5, "{} audio resyncs in 60 s", r.aud_snaps);
    }

    #[test]
    fn jittery_audio_stays_locked() {
        // Audio arrival jitters by up to +/-30 ms around its nominal time.
        let mut events = video(0, 60_000_000, 1_000_000 / 15);
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        for i in 1..(60_000_000 / AAC_US as i64) {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let jitter = ((seed >> 33) % 60_001) as i64 - 30_000;
            events.push((i * AAC_US as i64 + jitter, Ev::Audio));
        }
        let r = run(events);
        assert_eq!(r.vid_back, 0);
        assert_eq!(r.aud_back, 0);
        assert!(r.aud_snaps <= 5, "{} audio resyncs in 60 s", r.aud_snaps);
    }

    #[test]
    fn slow_camera_follows_its_own_timestamps() {
        // The camera is configured for 15 fps but delivers fewer.
        for real_fps in [8, 10, 12, 14] {
            let mut events = video(0, 60_000_000, 1_000_000 / real_fps);
            events.extend(audio(60_000_000));
            let r = run(events);
            assert_eq!(r.vid_back, 0, "{real_fps} fps");
            assert_eq!(r.aud_back, 0, "{real_fps} fps");
            assert_eq!(r.vid_snaps, 0, "{real_fps} fps");
            assert!(r.aud_snaps <= 2, "{real_fps} fps: {} resyncs", r.aud_snaps);
        }
    }
}
