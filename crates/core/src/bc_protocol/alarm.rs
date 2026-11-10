use super::{BcCamera, Error, Result};
use crate::bc::{model::*, xml::*};
use tokio::sync::mpsc::{channel, error::TryRecvError, Receiver};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// A handle on the raw alarm events coming from the camera
///
/// Unlike [`super::MotionData`] the events are not reduced to a start or
/// stop status. Each [`AlarmEvent`] for this channel is passed on as the
/// camera sent it, so fields such as `AItype` reach the caller.
///
/// When this object is dropped the alarm events are stopped
pub struct AlarmData {
    handle: JoinSet<Result<()>>,
    cancel: CancellationToken,
    rx: Receiver<Result<AlarmEvent>>,
}

impl AlarmData {
    /// Consume the alarm events that have already arrived, without waiting
    ///
    /// An error is raised if the alarm connection to the camera is dropped
    pub fn consume_alarm_events(&mut self) -> Result<Vec<AlarmEvent>> {
        let mut results: Vec<AlarmEvent> = vec![];
        loop {
            match self.rx.try_recv() {
                Ok(alarm) => results.push(alarm?),
                Err(TryRecvError::Empty) => break,
                Err(e) => return Err(Error::from(e)),
            }
        }
        Ok(results)
    }

    /// Await the next alarm event, in the order the camera sent them
    ///
    /// An error is raised if the alarm connection to the camera is dropped
    pub async fn next_alarm(&mut self) -> Result<AlarmEvent> {
        match self.rx.recv().await {
            Some(alarm) => alarm,
            None => Err(Error::Other("Alarm dropped")),
        }
    }
}

/// Take the alarm events for one channel out of a message 33 body
///
/// Bodies without an `AlarmEventList` give an empty list.
fn alarm_events_for_channel(body: BcBody, channel_id: u8) -> Vec<AlarmEvent> {
    if let BcBody::ModernMsg(ModernMsg {
        payload:
            Some(BcPayloads::BcXml(BcXml {
                alarm_event_list: Some(alarm_event_list),
                ..
            })),
        ..
    }) = body
    {
        alarm_event_list
            .alarm_events
            .into_iter()
            .filter(|alarm_event| alarm_event.channel_id == channel_id)
            .collect()
    } else {
        vec![]
    }
}

impl BcCamera {
    /// This returns a data structure which can be used to
    /// receive the raw alarm events of this channel
    ///
    /// It sends the same request as [`BcCamera::listen_on_motion`] and
    /// reads the same message 33, but passes on every [`AlarmEvent`]
    /// unchanged. Use it when the AI detection type is needed.
    ///
    /// A connection allows one live subscriber per message id, so this and
    /// [`BcCamera::listen_on_motion`] cannot run at the same time on one
    /// camera connection: the second gets `Error::SimultaneousSubscription`.
    pub async fn listen_on_alarms(&self) -> Result<AlarmData> {
        self.start_motion_query().await?;

        let connection = self.get_connection();

        // After start_motion_query (MSG_ID 31) the camera sends alarm
        // messages on MSG_ID 33 whenever an alarm changes.
        let (tx, rx) = channel(20);

        let mut set = JoinSet::new();
        let channel_id = self.channel_id;
        let cancel = CancellationToken::new();
        let thread_cancel = cancel.clone();
        set.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    let mut sub = connection.subscribe_to_id(MSG_ID_MOTION).await?;

                    loop {
                        tokio::task::yield_now().await;
                        match sub.recv().await {
                            Ok(alarm_msg) => {
                                for alarm_event in alarm_events_for_channel(alarm_msg.body, channel_id) {
                                    if tx.send(Ok(alarm_event)).await.is_err() {
                                        // Alarm reciever has been dropped
                                        return Ok(());
                                    }
                                }
                            }
                            // On connection drop we stop
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                return Ok(());
                            }
                        }
                    }
                } => v,
            }
        });

        Ok(AlarmData {
            handle: set,
            cancel,
            rx,
        })
    }
}

impl Drop for AlarmData {
    fn drop(&mut self) {
        log::trace!("Drop AlarmData");
        self.cancel.cancel();
        let mut handle = std::mem::take(&mut self.handle);
        let _gt = tokio::runtime::Handle::current().enter();
        tokio::task::spawn(async move {
            while handle.join_next().await.is_some() {}
            log::trace!("Dropped AlarmData");
        });
    }
}

#[cfg(test)]
mod tests {
    //! Every XML fixture in these tests is SYNTHETIC. None was captured from a
    //! camera. They are written by hand to the field names of [`AlarmEvent`].
    use super::*;
    use indoc::indoc;

    fn body_from_xml(xml: &str) -> BcBody {
        BcBody::ModernMsg(ModernMsg {
            extension: None,
            payload: Some(BcPayloads::BcXml(
                BcXml::try_parse(xml.as_bytes()).expect("synthetic fixture should parse"),
            )),
        })
    }

    // Synthetic: one event carrying every known AItype value
    const SYNTHETIC_ALL_CLASSES: &str = indoc!(
        r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <AlarmEventList version="1.1">
        <AlarmEvent version="1.1">
        <channelId>0</channelId>
        <status>MD</status>
        <AItype>people,vehicle,dog_cat,package,non-motor vehicle,other</AItype>
        <recording>1</recording>
        <timeStamp>1762700002</timeStamp>
        </AlarmEvent>
        </AlarmEventList>
        </body>"#
    );

    // Synthetic: an NVR message with two channels
    const SYNTHETIC_TWO_CHANNELS: &str = indoc!(
        r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <AlarmEventList version="1.1">
        <AlarmEvent version="1.1">
        <channelId>0</channelId>
        <status>MD</status>
        <AItype>people</AItype>
        <recording>1</recording>
        <timeStamp>1762700004</timeStamp>
        </AlarmEvent>
        <AlarmEvent version="1.1">
        <channelId>1</channelId>
        <status>none</status>
        <AItype>vehicle</AItype>
        <recording>0</recording>
        <timeStamp>0</timeStamp>
        </AlarmEvent>
        </AlarmEventList>
        </body>"#
    );

    // Synthetic: an older camera that sends no AItype element
    const SYNTHETIC_NO_AITYPE: &str = indoc!(
        r#"
        <?xml version="1.0" encoding="UTF-8" ?>
        <body>
        <AlarmEventList version="1.1">
        <AlarmEvent version="1.1">
        <channelId>0</channelId>
        <status>MD</status>
        <recording>0</recording>
        <timeStamp>0</timeStamp>
        </AlarmEvent>
        </AlarmEventList>
        </body>"#
    );

    #[test]
    fn test_alarm_ai_type_passes_through() {
        let events = alarm_events_for_channel(body_from_xml(SYNTHETIC_ALL_CLASSES), 0);
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.status, "MD");
        assert_eq!(
            event.ai_type.as_deref(),
            Some("people,vehicle,dog_cat,package,non-motor vehicle,other")
        );
        assert_eq!(event.recording, 1);
        assert_eq!(event.timeStamp, 1762700002);
    }

    #[test]
    fn test_alarm_filters_by_channel() {
        let ch0 = alarm_events_for_channel(body_from_xml(SYNTHETIC_TWO_CHANNELS), 0);
        assert_eq!(ch0.len(), 1);
        assert_eq!(ch0[0].channel_id, 0);
        assert_eq!(ch0[0].ai_type.as_deref(), Some("people"));

        let ch1 = alarm_events_for_channel(body_from_xml(SYNTHETIC_TWO_CHANNELS), 1);
        assert_eq!(ch1.len(), 1);
        assert_eq!(ch1[0].status, "none");
        assert_eq!(ch1[0].ai_type.as_deref(), Some("vehicle"));

        assert!(alarm_events_for_channel(body_from_xml(SYNTHETIC_TWO_CHANNELS), 2).is_empty());
    }

    #[test]
    fn test_alarm_without_ai_type() {
        let events = alarm_events_for_channel(body_from_xml(SYNTHETIC_NO_AITYPE), 0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status, "MD");
        assert_eq!(events[0].ai_type, None);
    }

    #[test]
    fn test_alarm_ignores_other_bodies() {
        let empty = BcBody::ModernMsg(ModernMsg {
            extension: None,
            payload: None,
        });
        assert!(alarm_events_for_channel(empty, 0).is_empty());
        let no_list = BcBody::ModernMsg(ModernMsg {
            extension: None,
            payload: Some(BcPayloads::BcXml(BcXml::default())),
        });
        assert!(alarm_events_for_channel(no_list, 0).is_empty());
    }

    #[tokio::test]
    async fn test_alarm_data_forwards_in_order() {
        let (tx, rx) = channel(20);
        let mut data = AlarmData {
            handle: JoinSet::new(),
            cancel: CancellationToken::new(),
            rx,
        };
        let events = alarm_events_for_channel(body_from_xml(SYNTHETIC_TWO_CHANNELS), 0)
            .into_iter()
            .chain(alarm_events_for_channel(
                body_from_xml(SYNTHETIC_ALL_CLASSES),
                0,
            ));
        for event in events {
            tx.send(Ok(event.clone())).await.unwrap();
        }

        let first = data.next_alarm().await.unwrap();
        assert_eq!(first.ai_type.as_deref(), Some("people"));
        let rest = data.consume_alarm_events().unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(
            rest[0].ai_type.as_deref(),
            Some("people,vehicle,dog_cat,package,non-motor vehicle,other")
        );

        drop(tx);
        assert!(data.next_alarm().await.is_err());
    }
}
