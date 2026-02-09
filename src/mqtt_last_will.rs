use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use rumqttc::{AsyncClient, Event, Incoming, LastWill, MqttOptions, QoS};
use serde_json::json;

pub async fn mqtt_last_will_task(
    alive_flag: Arc<AtomicBool>,
    host: &str,
    port: u16,
    software_name: &str,
    client_id: &str,
    username: &str,
    password: &str,
) {
    // let software_name = "block_server";
    // let client_id = "cid06";
    let topic = format!("v1/device/{}/{}/status/notify", software_name, client_id);
    let topic = topic.as_str();

    // ===============================
    // 1. MQTT 连接参数
    // ===============================
    let mut mqttoptions = MqttOptions::new(
        client_id, // client_id（非常重要）
        host, port,
    );

    mqttoptions
        .set_keep_alive(Duration::from_secs(15))
        .set_credentials(username, password);

    let mut sleep_secs = 1;

    loop {
        if !alive_flag.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::info!("wait task_server online. sleep {}secs", sleep_secs);
            tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
            sleep_secs *= 2;
            sleep_secs = sleep_secs.min(120);
            continue;
        }

        // ===============================
        // 2. 定义遗嘱（LWT）
        // ===============================
        let will_payload = json!({
             "id": 1,
            "version": 1.0,
            "type": "status",
            "params": {
                "status": "offline",
                "reason": "unexpected_disconnect",
                "name": software_name,
                "id": client_id,
                "timestamp": chrono::Utc::now().timestamp()
            }

        })
        .to_string();

        let last_will = LastWill {
            topic: topic.into(),
            message: bytes::Bytes::copy_from_slice(will_payload.as_bytes()),
            qos: QoS::AtLeastOnce,
            retain: true,
        };

        // 👉 绑定遗嘱到 CONNECT
        mqttoptions.set_last_will(last_will);

        // ===============================
        // 3. 创建客户端
        // ===============================

        let (client, mut eventloop) = AsyncClient::new(mqttoptions.clone(), 10);

        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                    tracing::info!("MQTT connected: {:?}", ack);
                    // 这里就是“已连接”
                    break;
                }

                Ok(Event::Incoming(_)) => {}

                Ok(Event::Outgoing(_)) => {}

                Err(e) => {
                    tracing::warn!(
                        "mqtt connection error: {:?}. sleep {}secs and then re-connected",
                        e,
                        sleep_secs
                    );
                    sleep_secs *= 2;
                    sleep_secs = sleep_secs.min(120);
                    tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
                }
            }
        }

        // ===============================
        // 4. 服务“上线”主动声明 online
        // ===============================
        let online_payload = json!({
            "id": 1,
            "version": 1.0,
            "type": "status",
            "params": {
                "status": "online",
                "name": software_name,
                "id": client_id,
                "pid": std::process::id(),
                "timestamp": chrono::Utc::now().timestamp()
            }
        })
        .to_string();

        if let Err(e) = client
            .publish(
                topic,
                QoS::AtLeastOnce,
                true, // retain
                online_payload,
            )
            .await
        {
            tracing::error!("publish online msg to mqtt error. {}.", e);
        }

        sleep_secs = 1;

        // ===============================
        // 5. 正常运行事件循环
        // ===============================
        loop {
            if !alive_flag.load(std::sync::atomic::Ordering::SeqCst) {
                tracing::warn!("block server offline. break mqtt eventloop");

                if let Err(e) = client
                    .publish(
                        topic,
                        QoS::AtLeastOnce,
                        true, // retain
                        will_payload.clone(),
                    )
                    .await
                {
                    tracing::error!("publish last will to mqtt error. {}.", e);
                }

                break;
            }

            match eventloop.poll().await {
                Ok(Event::Incoming(_)) => {}
                Ok(Event::Outgoing(_)) => {}
                Err(e) => {
                    tracing::info!("MQTT error: {:?}", e);
                    break;
                }
            }

            // client.disconnect().await.unwrap();
        }
    }
}
