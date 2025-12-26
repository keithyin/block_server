use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, QoS};
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

    loop {
        if !alive_flag.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(2)).await;
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
                "ts": chrono::Utc::now().timestamp()
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
                "ts": chrono::Utc::now().timestamp()
            }
        })
        .to_string();

        match client
            .publish(
                topic,
                QoS::AtLeastOnce,
                true, // retain
                online_payload,
            )
            .await
        {
            Ok(_) => {
                tracing::info!("connected to mqtt");
            }
            Err(e) => {
                tracing::error!("publish to mqtt error. {}", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        // ===============================
        // 5. 正常运行事件循环
        // ===============================
        loop {
            if !alive_flag.load(std::sync::atomic::Ordering::SeqCst) {
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
        }
    }
}
